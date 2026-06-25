package cluster

import "testing"

func TestClusterAddrsEquivalent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b string
		want bool
	}{
		{":4586", "[::]:4586", true},
		{"[::]:4586", ":4586", true},
		{":4586", "0.0.0.0:4586", true},
		{"[::]:4586", "127.0.0.1:4586", true},
		{":4586", "127.0.0.1:4586", true},
		{"localhost:4586", "127.0.0.1:4586", true},
		{"[::1]:4586", "127.0.0.1:4586", true},
		{"127.0.0.1:4566", "127.0.0.1:4586", false},
		{":4566", ":4586", false},
		{"node-a.example.com:4586", "node-b.example.com:4586", false},
		{"10.0.0.1:4586", "10.0.0.2:4586", false},
	}
	for _, tc := range tests {
		if got := clusterAddrsEquivalent(tc.a, tc.b); got != tc.want {
			t.Errorf("clusterAddrsEquivalent(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
