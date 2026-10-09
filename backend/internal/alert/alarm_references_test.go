package alert

import (
	"go/ast"
	"go/token"
	"regexp"
	"strings"
	"testing"
)

// alarmReference matches operator text that sends the reader to another alarm
// by its type ID: "the retention-deferred alarm", "if vault-leaderless is also
// standing". An operator follows such a pointer by looking for that ID in the
// alarm list, so a pointer to an ID the catalog does not hold sends them
// looking for something that can never appear.
var alarmReference = regexp.MustCompile("\\b([a-z][a-z0-9]*(?:-[a-z0-9]+)+)`?\\s+(?:alarms?\\b|is (?:also )?standing\\b)")

// danglingAlarmReferences returns every alarm ID text refers to that names
// neither a catalog type nor a family of them ("disk-space" for
// disk-space-low and disk-space-exhausted).
func danglingAlarmReferences(text string) []string {
	var out []string
	for _, m := range alarmReference.FindAllStringSubmatch(text, -1) {
		if !namesCatalogedAlarm(m[1]) {
			out = append(out, m[1])
		}
	}
	return out
}

func namesCatalogedAlarm(ref string) bool {
	if _, ok := TypeByID(ref); ok {
		return true
	}
	for _, typ := range catalog {
		if strings.HasPrefix(typ.IDPrefix, ref+"-") {
			return true
		}
	}
	return false
}

func TestCatalogGuidanceReferencesOnlyCatalogedAlarms(t *testing.T) {
	t.Parallel()
	for _, typ := range Types() {
		for field, text := range map[string]string{"Cause": typ.Cause, "Response": typ.Response} {
			for _, ref := range danglingAlarmReferences(text) {
				t.Errorf("%s %s refers to alarm %q, which is not in the catalog", typ.IDPrefix, field, ref)
			}
		}
	}
}

// Alarm details are mostly built at raise sites a test cannot reach without a
// live cluster, and often assembled across helpers, so this checks every string
// literal in the backend tree: detail text, log lines and CLI help alike send
// an operator to an alarm by name.
func TestSourceTextReferencesOnlyCatalogedAlarms(t *testing.T) {
	t.Parallel()
	scanned := 0
	forEachSourcePackage(t, func(fset *token.FileSet, pkg *ast.Package) {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				text, ok := literalString(asExpr(n))
				if !ok {
					return true
				}
				scanned++
				for _, ref := range danglingAlarmReferences(text) {
					t.Errorf("%s refers to alarm %q, which is not in the catalog", sourcePosition(fset, n.Pos()), ref)
				}
				return true
			})
		}
	})
	const minScanned = 1000
	if scanned < minScanned {
		t.Fatalf("scanned only %d string literals (want >= %d) — the scanner is no longer reading the tree, so this test proves nothing",
			scanned, minScanned)
	}
}

func asExpr(n ast.Node) ast.Expr {
	e, _ := n.(ast.Expr)
	return e
}

// The reference matcher must recognize the phrasings alarm text actually uses,
// or the two tests above pass vacuously.
func TestDanglingAlarmReferencesRecognizesPhrasings(t *testing.T) {
	t.Parallel()
	cases := []struct {
		text string
		want []string
	}{
		{"Read the retention-deferred alarm for why.", nil},
		{"If retention-deferred is also standing, act on it.", nil},
		{"raises the disk-space alarm below the warn band", nil},
		{"Read the retention-stalled alarm for why.", []string{"retention-stalled"}},
		{"If retention-stalled is standing, act on it.", []string{"retention-stalled"}},
		{"until free space clears its low-disk alarm band", []string{"low-disk"}},
		{"see the `vault-gone` alarms", []string{"vault-gone"}},
		{"a drain-only policy never refuses", nil},
	}
	for _, c := range cases {
		got := danglingAlarmReferences(c.text)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("danglingAlarmReferences(%q) = %v, want %v", c.text, got, c.want)
		}
	}
}
