package orchestrator

import (
	"fmt"
	"time"

	"gastrolog/internal/glid"
)

// vaultLeaderlessAlarmAfter is the delay-on time before a leaderless vault
// raises an alarm: placement edits and mid-flap partial states legitimately
// resolve to no leader for a tick or two, and the role-reconcile sweep heals
// those on its own. Four sweep ticks of sustained leaderlessness means the
// config itself resolves to no leader — auto-healing cannot fix that, so an
// operator has to. (Delay-on rather than raise-and-clear keeps the alarm
// list from chattering during placement changes — EEMUA 191.)
const vaultLeaderlessAlarmAfter = 60 * time.Second

// noteVaultLeaderless records this sweep tick's leader-resolution outcome
// for one vault and maintains the vault-leaderless alarm. leaderless vaults
// alarm after the delay-on window; a vault that resolves (or whose instance
// is gone) clears immediately.
//
// Called from placementSweep with the full outcome map each tick.
func (o *Orchestrator) updateLeaderlessAlarms(now time.Time, leaderless map[glid.GLID]string) {
	if o.alerts == nil {
		return
	}
	o.leaderlessMu.Lock()
	defer o.leaderlessMu.Unlock()
	if o.leaderlessSince == nil {
		o.leaderlessSince = make(map[glid.GLID]time.Time)
	}
	for vaultID, name := range leaderless {
		since, ok := o.leaderlessSince[vaultID]
		if !ok {
			o.leaderlessSince[vaultID] = now
			continue
		}
		if now.Sub(since) < vaultLeaderlessAlarmAfter {
			continue
		}
		o.alerts.Raise("vault-leaderless", vaultID.String(),
			fmt.Sprintf("Vault %s has had no placement leader for %s.",
				name, now.Sub(since).Round(time.Second)))
	}
	for vaultID := range o.leaderlessSince {
		if _, still := leaderless[vaultID]; still {
			continue
		}
		delete(o.leaderlessSince, vaultID)
		o.alerts.Clear("vault-leaderless", vaultID.String())
	}
}
