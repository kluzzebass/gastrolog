package orchestrator

import (
	"fmt"

	"gastrolog/internal/glid"
)

// updateLeaderlessAlarms reports this sweep tick's full leader-resolution
// outcome to the alarm collector. Chattering suppression is the catalog's:
// vault-leaderless carries a 60s DelayOn (placement edits and mid-flap
// partial states legitimately resolve to no leader for a tick or two, and
// the role-reconcile sweep heals those on its own — sustained
// leaderlessness means the config itself resolves to no leader, which only
// an operator can fix), so the sweep just raises the raw condition every
// tick and the collector holds it back until it persists. A vault that
// resolves — or whose instance left this node's registry — clears
// immediately.
//
// leaderlessReported tracks only set membership (which vaults were reported
// leaderless last pass) so departures diff to a Clear; the per-vault clocks
// that used to live here moved into the collector with the suppression
// phase (gastrolog-4wvxqh).
//
// Called from ReconcilePlacements with the full outcome map for the cluster-
// wide reconcile (NSC change, config replay). Single-vault placement events
// use updateLeaderlessAlarm instead.
func (o *Orchestrator) updateLeaderlessAlarms(leaderless map[glid.GLID]string) {
	if o.alerts == nil {
		return
	}
	o.leaderlessMu.Lock()
	defer o.leaderlessMu.Unlock()
	if o.leaderlessReported == nil {
		o.leaderlessReported = make(map[glid.GLID]struct{})
	}
	for vaultID, name := range leaderless {
		o.leaderlessReported[vaultID] = struct{}{}
		o.alerts.Raise("vault-leaderless", vaultID.String(),
			fmt.Sprintf("Vault %s has no placement leader.", name))
	}
	for vaultID := range o.leaderlessReported {
		if _, still := leaderless[vaultID]; still {
			continue
		}
		delete(o.leaderlessReported, vaultID)
		o.alerts.Clear("vault-leaderless", vaultID.String())
	}
}

// updateLeaderlessAlarm is the single-vault form driven by ReconcileVaultPlacement
// on a placement event: it raises (leaderless) or clears (resolved) the
// vault-leaderless alarm for one vault and keeps leaderlessReported in sync so
// the cluster-wide updateLeaderlessAlarms pass and this event path share one
// membership set. The collector's 60s DelayOn is evaluated lazily on read
// (see alert.Collector), so a raise fired once on the event annunciates on its
// own once the window elapses — no periodic re-raise from a sweep needed.
func (o *Orchestrator) updateLeaderlessAlarm(vaultID glid.GLID, name string, leaderless bool) {
	if o.alerts == nil {
		return
	}
	o.leaderlessMu.Lock()
	defer o.leaderlessMu.Unlock()
	if o.leaderlessReported == nil {
		o.leaderlessReported = make(map[glid.GLID]struct{})
	}
	if leaderless {
		o.leaderlessReported[vaultID] = struct{}{}
		o.alerts.Raise("vault-leaderless", vaultID.String(),
			fmt.Sprintf("Vault %s has no placement leader.", name))
		return
	}
	delete(o.leaderlessReported, vaultID)
	o.alerts.Clear("vault-leaderless", vaultID.String())
}
