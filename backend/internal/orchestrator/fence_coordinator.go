package orchestrator

import (
	"context"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

type vaultFenceCoordinator struct {
	vaultID glid.GLID
	o       *Orchestrator
	hints   fenceHintArbitrator
}

func (o *Orchestrator) fenceCoordinator(vaultID glid.GLID) *vaultFenceCoordinator {
	if v, ok := o.fenceCoords.Load(vaultID); ok {
		return v.(*vaultFenceCoordinator)
	}
	c := &vaultFenceCoordinator{
		vaultID: vaultID,
		o:       o,
		hints:   *newFenceHintArbitrator(),
	}
	actual, _ := o.fenceCoords.LoadOrStore(vaultID, c)
	return actual.(*vaultFenceCoordinator)
}

// SubmitFenceHint ingests ephemeral replica evidence on this node.
func (o *Orchestrator) SubmitFenceHint(vaultID glid.GLID, hint FenceHint) bool {
	if o.vaultWriteModel(vaultID) != system.VaultWriteModelSequenced {
		return false
	}
	return o.fenceCoordinator(vaultID).hints.Ingest(hint)
}

func (o *Orchestrator) submitLocalFenceHint(vaultID glid.GLID, at time.Time) {
	nodeID := o.localNodeID
	if nodeID == "" {
		nodeID = "local"
	}
	h := uint64(0)
	if ss := o.vaultSpoolStore(vaultID); ss != nil {
		h = ss.IngestHighWatermark()
	}
	if h == 0 {
		return
	}
	o.SubmitFenceHint(vaultID, FenceHint{NodeID: nodeID, H: h, ObservedAt: at})
}

func (c *vaultFenceCoordinator) authoritativeH() uint64 {
	local := uint64(0)
	if ss := c.o.vaultSpoolStore(c.vaultID); ss != nil {
		local = ss.IngestHighWatermark()
	}
	hintH := c.hints.EffectiveH()
	if local > hintH {
		return local
	}
	return hintH
}

func (c *vaultFenceCoordinator) latestPublishedFence() uint64 {
	sub, err := c.o.vaultCtlSubFSM(c.vaultID)
	if err != nil || sub == nil {
		return 0
	}
	return sub.LatestFenceUpperBound()
}

func (c *vaultFenceCoordinator) countFenceTarget(maxRecords int64, prev, h uint64) (uint64, bool) {
	if maxRecords <= 0 {
		return 0, false
	}
	target := prev + uint64(maxRecords)
	if h < target {
		return 0, false
	}
	return target, true
}

// evaluateAndPublish applies rotation-policy fence rules and durably publishes
// when H (local or hinted) satisfies the next fence boundary. Fence cuts use
// accepted seq labels (H), not contiguous slot presence — unassigned gaps from
// burned swaths do not block publication.
func (c *vaultFenceCoordinator) evaluateAndPublish(at time.Time, policy *system.RotationPolicyConfig, timeTriggered bool) error {
	if policy == nil {
		return nil
	}
	prev := c.latestPublishedFence()
	h := c.authoritativeH()
	if h <= prev {
		return nil
	}

	var upper uint64
	var ok bool
	if policy.MaxRecords != nil {
		upper, ok = c.countFenceTarget(*policy.MaxRecords, prev, h)
	}
	if !ok && timeTriggered {
		upper, ok = h, true
	}
	if !ok {
		return nil
	}
	return c.publishFence(upper, at)
}

func (c *vaultFenceCoordinator) publishFence(upperBoundSeq uint64, at time.Time) error {
	wire := vaultraft.MarshalVaultPublishFence(c.vaultID, upperBoundSeq, at)
	if c.o.groupMgr != nil {
		return c.o.ApplyVaultControlPlane(c.vaultID, wire)
	}
	if fsm := c.o.testSeqFSM[c.vaultID]; fsm != nil {
		payload := wire[1+glid.Size:]
		result := fsm.Apply(&hraft.Log{Data: payload})
		if err, ok := result.(error); ok {
			return err
		}
		return nil
	}
	return ErrSeqAssignUnavailable
}

// EvaluateVaultFenceAfterHint re-runs fence publication for vault-ctl leaders
// after accepting remote replica hint evidence. Avoids waiting for the next
// rotation-sweep tick when cross-node hints arrive via NodeStats broadcast.
func (o *Orchestrator) EvaluateVaultFenceAfterHint(vaultID glid.GLID) error {
	return o.evaluateVaultFence(vaultID, o.now(), false)
}

func (o *Orchestrator) evaluateVaultFence(vaultID glid.GLID, at time.Time, timeTriggered bool) error {
	if o.vaultWriteModel(vaultID) != system.VaultWriteModelSequenced {
		return nil
	}
	if !o.isVaultCtlLeader(vaultID) {
		return nil
	}
	sys, err := o.loadSystem(context.Background())
	if err != nil {
		return err
	}
	vaultCfg := findVaultConfig(sys.Config.Vaults, vaultID)
	if vaultCfg == nil || vaultCfg.RotationPolicyID == nil {
		return nil
	}
	policy := findRotationPolicy(sys.Config.RotationPolicies, *vaultCfg.RotationPolicyID)
	if policy == nil {
		return nil
	}
	return o.fenceCoordinator(vaultID).evaluateAndPublish(at, policy, timeTriggered)
}

func (o *Orchestrator) isVaultCtlLeader(vaultID glid.GLID) bool {
	if o.groupMgr == nil {
		return true
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return false
	}
	return g.Raft.State() == hraft.Leader
}

func (o *Orchestrator) vaultFenceHighWatermark(vaultID glid.GLID) uint64 {
	sub, err := o.vaultCtlSubFSM(vaultID)
	if err != nil || sub == nil {
		return 0
	}
	return sub.LatestFenceUpperBound()
}

// FenceState returns durable published fence history for operator inspection.
func (o *Orchestrator) FenceState(vaultID glid.GLID) vaultctlfsm.FenceSnapshot {
	sub, err := o.vaultCtlSubFSM(vaultID)
	if err != nil || sub == nil {
		return vaultctlfsm.FenceSnapshot{}
	}
	return sub.FenceState()
}
