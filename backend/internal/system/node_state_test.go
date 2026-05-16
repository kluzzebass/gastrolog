package system

import "testing"

func TestNodeState_EffectiveState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw  NodeState
		want NodeState
	}{
		{NodeStateUnknown, NodeStateLive}, // legacy/zero-value lazy mapping
		{NodeStateLive, NodeStateLive},
		{NodeStateUnreachable, NodeStateUnreachable},
		{NodeStateMaintenance, NodeStateMaintenance},
		{NodeStateDraining, NodeStateDraining},
		{NodeStateDecommissioning, NodeStateDecommissioning},
	}
	for _, tc := range tests {
		n := NodeConfig{State: tc.raw}
		if got := n.EffectiveState(); got != tc.want {
			t.Errorf("EffectiveState(raw=%s): got %s, want %s", tc.raw, got, tc.want)
		}
	}
}

func TestNodeState_String(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s    NodeState
		want string
	}{
		{NodeStateUnknown, "unknown"},
		{NodeStateLive, "live"},
		{NodeStateUnreachable, "unreachable"},
		{NodeStateMaintenance, "maintenance"},
		{NodeStateDraining, "draining"},
		{NodeStateDecommissioning, "decommissioning"},
	}
	for _, tc := range tests {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("String(%d): got %q, want %q", tc.s, got, tc.want)
		}
	}
}

func TestValidateNodeStateTransition_Legal(t *testing.T) {
	t.Parallel()
	legal := []struct {
		from, to NodeState
	}{
		// Live → soft-offline + draining.
		{NodeStateLive, NodeStateUnreachable},
		{NodeStateLive, NodeStateMaintenance},
		{NodeStateLive, NodeStateDraining},

		// Unreachable → live (recover) + maintenance + draining.
		{NodeStateUnreachable, NodeStateLive},
		{NodeStateUnreachable, NodeStateMaintenance},
		{NodeStateUnreachable, NodeStateDraining},

		// Maintenance → live (operator clear) + draining.
		{NodeStateMaintenance, NodeStateLive},
		{NodeStateMaintenance, NodeStateDraining},

		// Draining → decommissioning (complete) + live (cancel).
		{NodeStateDraining, NodeStateDecommissioning},
		{NodeStateDraining, NodeStateLive},

		// Lazy migration: legacy Unknown is treated as Live for the
		// transition check, so every Live-successor is legal from
		// Unknown too.
		{NodeStateUnknown, NodeStateUnreachable},
		{NodeStateUnknown, NodeStateMaintenance},
		{NodeStateUnknown, NodeStateDraining},

		// Idempotent: re-applying the same state is a no-op success.
		{NodeStateLive, NodeStateLive},
		{NodeStateUnreachable, NodeStateUnreachable},
		{NodeStateMaintenance, NodeStateMaintenance},
		{NodeStateDraining, NodeStateDraining},
		{NodeStateDecommissioning, NodeStateDecommissioning},
	}
	for _, tc := range legal {
		if err := ValidateNodeStateTransition(tc.from, tc.to); err != nil {
			t.Errorf("ValidateNodeStateTransition(%s → %s): expected legal, got error: %v",
				tc.from, tc.to, err)
		}
	}
}

func TestValidateNodeStateTransition_Illegal(t *testing.T) {
	t.Parallel()
	illegal := []struct {
		from, to NodeState
	}{
		// Skipping Draining: Live → Decommissioning isn't legal.
		{NodeStateLive, NodeStateDecommissioning},
		{NodeStateUnreachable, NodeStateDecommissioning},
		{NodeStateMaintenance, NodeStateDecommissioning},

		// Maintenance → Unreachable: don't downgrade operator-sticky
		// state to cluster-auto-detected.
		{NodeStateMaintenance, NodeStateUnreachable},

		// Decommissioning has no in-FSM successor (Removed = DeleteNode).
		{NodeStateDecommissioning, NodeStateLive},
		{NodeStateDecommissioning, NodeStateUnreachable},
		{NodeStateDecommissioning, NodeStateMaintenance},
		{NodeStateDecommissioning, NodeStateDraining},
	}
	for _, tc := range illegal {
		if err := ValidateNodeStateTransition(tc.from, tc.to); err == nil {
			t.Errorf("ValidateNodeStateTransition(%s → %s): expected illegal, got nil",
				tc.from, tc.to)
		}
	}
}
