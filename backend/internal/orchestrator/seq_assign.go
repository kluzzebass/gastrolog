package orchestrator

import (
	"errors"
	"fmt"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

const defaultSeqLeaseBatch = 256

// vaultSeqLease tracks the orchestrator's locally consumed prefix of the
// active allocator lease granted via vault-ctl Raft.
type vaultSeqLease struct {
	epoch uint64
	next  uint64
	end   uint64 // inclusive; zero means no active local lease
}

// ErrSeqAssignUnavailable is returned when seq assignment cannot reach
// vault-ctl allocator authority (no Raft group and no test backend).
var ErrSeqAssignUnavailable = errors.New("seq assign: vault-ctl allocator unavailable")

// ErrSeqAssignNoActiveLease is returned when reserve succeeds but FSM state lacks a lease.
var ErrSeqAssignNoActiveLease = errors.New("seq assign: reserve applied but no active lease")

// ErrSequencedChunkAppendForbidden is returned when a sequenced vault record
// reaches the chunk Append entry point without a pre-assigned VaultSeq.
var ErrSequencedChunkAppendForbidden = errors.New("sequenced vault: chunk append forbidden; assign VaultSeq on ingesting router")

func (o *Orchestrator) assignDestinationVaultSeq(vaultID glid.GLID, _ chunk.EventID) (uint64, error) {
	return o.consumeNextVaultSeq(vaultID)
}

func (o *Orchestrator) consumeNextVaultSeq(vaultID glid.GLID) (uint64, error) {
	v := o.vaults[vaultID]
	if v == nil {
		return 0, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	lease := &v.seqLease

	if err := o.ensureLocalSeqLease(vaultID, lease); err != nil {
		return 0, err
	}
	if lease.next == 0 || lease.next > lease.end {
		if err := o.renewLocalSeqLease(vaultID, lease); err != nil {
			return 0, err
		}
	}
	seq := lease.next
	lease.next++
	return seq, nil
}

func (o *Orchestrator) ensureLocalSeqLease(vaultID glid.GLID, lease *vaultSeqLease) error {
	epoch, err := o.currentAllocatorEpoch(vaultID)
	if err != nil {
		return err
	}
	if lease.epoch != epoch {
		*lease = vaultSeqLease{epoch: epoch}
	}
	if lease.end == 0 || lease.next > lease.end {
		return o.renewLocalSeqLease(vaultID, lease)
	}
	return nil
}

func (o *Orchestrator) renewLocalSeqLease(vaultID glid.GLID, lease *vaultSeqLease) error {
	if lease.end > 0 && lease.next <= lease.end {
		if err := o.burnVaultSeqLeaseTail(vaultID, lease.epoch, lease.end); err != nil {
			return err
		}
	}
	grant, err := o.reserveVaultSeqRange(vaultID, lease.epoch, defaultSeqLeaseBatch)
	if err != nil {
		return err
	}
	lease.epoch = grant.Epoch
	lease.next = grant.Start
	lease.end = grant.End
	if v := o.vaults[vaultID]; v != nil {
		if err := v.ensureSpoolStore(o).EnsureSwathWindow(grant.Start, grant.End); err != nil {
			return err
		}
	}
	return nil
}

func (o *Orchestrator) currentAllocatorEpoch(vaultID glid.GLID) (uint64, error) {
	sub, err := o.vaultCtlSubFSM(vaultID)
	if err != nil {
		return 0, err
	}
	if sub == nil {
		return vaultctlfsm.InitialSeqEpoch, nil
	}
	return sub.SeqAllocatorState().Epoch, nil
}

func (o *Orchestrator) reserveVaultSeqRange(vaultID glid.GLID, epoch, count uint64) (vaultctlfsm.SeqLeaseGrant, error) {
	holder := o.localNodeID
	if holder == "" {
		holder = "local"
	}
	wire, err := vaultraft.MarshalVaultReserveSeqRange(vaultID, holder, epoch, count)
	if err != nil {
		return vaultctlfsm.SeqLeaseGrant{}, err
	}
	if o.groupMgr != nil {
		return o.reserveVaultSeqRangeRaft(vaultID, wire, holder, epoch)
	}
	if fsm := o.testSeqFSM[vaultID]; fsm != nil {
		return o.reserveVaultSeqRangeTestFSM(fsm, wire)
	}
	return vaultctlfsm.SeqLeaseGrant{}, ErrSeqAssignUnavailable
}

func (o *Orchestrator) reserveVaultSeqRangeRaft(vaultID glid.GLID, wire []byte, holder string, epoch uint64) (vaultctlfsm.SeqLeaseGrant, error) {
	if err := o.ApplyVaultControlPlane(vaultID, wire); err != nil {
		return vaultctlfsm.SeqLeaseGrant{}, err
	}
	if grant, ok := o.lookupSeqLeaseGrant(vaultID, holder, epoch); ok {
		return grant, nil
	}
	deadline := time.Now().Add(cluster.ReplicationTimeout)
	for time.Now().Before(deadline) {
		if grant, ok := o.lookupSeqLeaseGrant(vaultID, holder, epoch); ok {
			return grant, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return vaultctlfsm.SeqLeaseGrant{}, ErrSeqAssignNoActiveLease
}

func (o *Orchestrator) reserveVaultSeqRangeTestFSM(fsm *vaultctlfsm.FSM, wire []byte) (vaultctlfsm.SeqLeaseGrant, error) {
	result := fsm.Apply(&hraft.Log{Data: wire[1+glid.Size:]})
	switch r := result.(type) {
	case vaultctlfsm.SeqLeaseGrant:
		return r, nil
	case error:
		return vaultctlfsm.SeqLeaseGrant{}, r
	default:
		return vaultctlfsm.SeqLeaseGrant{}, fmt.Errorf("seq assign: unexpected reserve response %T", result)
	}
}

func (o *Orchestrator) lookupSeqLeaseGrant(vaultID glid.GLID, holder string, epoch uint64) (vaultctlfsm.SeqLeaseGrant, bool) {
	sub, err := o.vaultCtlSubFSM(vaultID)
	if err != nil || sub == nil {
		return vaultctlfsm.SeqLeaseGrant{}, false
	}
	for _, sw := range sub.SeqAllocatorState().ActiveSwaths {
		if sw.HolderID == holder && sw.Epoch == epoch {
			return vaultctlfsm.SeqLeaseGrant{
				Start: sw.RangeStart,
				End:   sw.RangeEnd,
				Epoch: sw.Epoch,
			}, true
		}
	}
	return vaultctlfsm.SeqLeaseGrant{}, false
}

func (o *Orchestrator) burnVaultSeqLeaseTail(vaultID glid.GLID, epoch, consumedEnd uint64) error {
	holder := o.localNodeID
	if holder == "" {
		holder = "local"
	}
	wire, err := vaultraft.MarshalVaultBurnSeqLeaseTail(vaultID, holder, epoch, consumedEnd)
	if err != nil {
		return err
	}
	return o.applyVaultSeqWire(vaultID, wire)
}

func (o *Orchestrator) bumpVaultSeqAllocatorEpoch(vaultID glid.GLID) error {
	return o.applyVaultSeqWire(vaultID, vaultraft.MarshalVaultBumpSeqAllocatorEpoch(vaultID))
}

func (o *Orchestrator) applyVaultSeqWire(vaultID glid.GLID, wire []byte) error {
	if o.groupMgr != nil {
		return o.ApplyVaultControlPlane(vaultID, wire)
	}
	if fsm := o.testSeqFSM[vaultID]; fsm != nil {
		payload := wire[1+glid.Size:]
		if result := fsm.Apply(&hraft.Log{Data: payload}); result != nil {
			if err, ok := result.(error); ok {
				return err
			}
		}
		return nil
	}
	return ErrSeqAssignUnavailable
}

func (o *Orchestrator) vaultCtlSubFSM(vaultID glid.GLID) (*vaultctlfsm.FSM, error) {
	if fsm := o.testSeqFSM[vaultID]; fsm != nil {
		return fsm, nil
	}
	if o.groupMgr == nil {
		return nil, nil
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return nil, nil
	}
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		return raw, nil
	case *vaultraft.FSM:
		return raw.EnsureVaultFSM(vaultID), nil
	default:
		return nil, nil
	}
}

// wireTestSeqAllocator attaches a local vaultctl FSM for seq assignment in
// unit tests without a full vault-ctl Raft group.
func wireTestSeqAllocator(o *Orchestrator, vaultID glid.GLID) *vaultctlfsm.FSM {
	if o.testSeqFSM == nil {
		o.testSeqFSM = make(map[glid.GLID]*vaultctlfsm.FSM)
	}
	fsm := vaultctlfsm.New()
	o.testSeqFSM[vaultID] = fsm
	return fsm
}
