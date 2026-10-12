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
		// Live → soft-offline.
		{NodeStateLive, NodeStateUnreachable},
		{NodeStateLive, NodeStateMaintenance},

		// Unreachable → live (recover) + maintenance.
		{NodeStateUnreachable, NodeStateLive},
		{NodeStateUnreachable, NodeStateMaintenance},

		// Maintenance → live (operator clear).
		{NodeStateMaintenance, NodeStateLive},

		// Lazy migration: legacy Unknown is treated as Live for the
		// transition check, so every Live-successor is legal from
		// Unknown too.
		{NodeStateUnknown, NodeStateUnreachable},
		{NodeStateUnknown, NodeStateMaintenance},

		// Idempotent: re-applying the same state is a no-op success.
		{NodeStateLive, NodeStateLive},
		{NodeStateUnreachable, NodeStateUnreachable},
		{NodeStateMaintenance, NodeStateMaintenance},
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
	// The values the retired draining and decommissioning states had: no
	// state reaches them, so a stale or crafted request cannot put a node
	// into a state nothing handles.
	retired := []NodeState{NodeStateMaintenance + 1, NodeStateMaintenance + 2}
	illegal := []struct {
		from, to NodeState
	}{
		// Maintenance → Unreachable: don't downgrade operator-sticky
		// state to cluster-auto-detected.
		{NodeStateMaintenance, NodeStateUnreachable},
	}
	for _, from := range []NodeState{NodeStateUnknown, NodeStateLive, NodeStateUnreachable, NodeStateMaintenance} {
		for _, to := range retired {
			illegal = append(illegal, struct{ from, to NodeState }{from, to})
		}
	}
	for _, tc := range illegal {
		if err := ValidateNodeStateTransition(tc.from, tc.to); err == nil {
			t.Errorf("ValidateNodeStateTransition(%s → %s): expected illegal, got nil",
				tc.from, tc.to)
		}
	}
}
