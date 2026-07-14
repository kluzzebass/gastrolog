package orchestrator

import (
	"strings"
	"testing"
)

// diff powers the rebuild log line (3mnjlo boot-flap diagnosis): it must
// name changed fields, report changed params by KEY only (values may hold
// credentials), and stay stable-ordered.
func TestIngesterInfoDiff(t *testing.T) {
	t.Parallel()
	base := ingesterInfo{Name: "sb", Type: "scatterbox", Passive: false,
		Params: map[string]string{"rate": "100", "token": "secret"}}

	if got := base.diff(base); got != "none" {
		t.Errorf("identical infos diff = %q, want none", got)
	}
	got := base.diff(ingesterInfo{Name: "sb2", Type: "scatterbox", Passive: true,
		Params: map[string]string{"rate": "100", "token": "secret"}})
	if got != "name,passive" {
		t.Errorf("diff = %q, want name,passive", got)
	}
	got = base.diff(ingesterInfo{Name: "sb", Type: "scatterbox",
		Params: map[string]string{"rate": "200", "burst": "5"}})
	if got != "params[burst,rate,token]" {
		t.Errorf("param diff = %q, want params[burst,rate,token]", got)
	}
	if strings.Contains(got, "secret") || strings.Contains(got, "200") {
		t.Errorf("diff leaked a param value: %q", got)
	}
}
