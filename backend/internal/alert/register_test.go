package alert

import (
	"regexp"
	"strings"
	"testing"
)

// Alarm text is read by an operator mid-incident. These tests pin the register
// rules from docs/ubiquitous_language.md ("Alarm register") against the whole
// catalog, so a new entry cannot reintroduce the class of defect the chunking
// entries carried: Go source notation, Go duration spelling, and a Critical
// alarm that leads with mechanism instead of the loss it announces.

// sourceNotation matches spellings that are code rather than prose.
var sourceNotation = []struct {
	name string
	re   *regexp.Regexp
}{
	// Any call-shaped token: min(...), len(...), foo(bar).
	{"a function call", regexp.MustCompile(`\b[a-z][A-Za-z0-9_]*\([^)]*\)`)},
	// Go duration spelling with a padded zero component: 3m0s, 12h0m0s, 1h0m30s.
	{"a padded Go duration", regexp.MustCompile(`\d+[hms]0[ms]`)},
	// camelCase and snake_case identifiers.
	{"a camelCase identifier", regexp.MustCompile(`\b[a-z]+[A-Z][A-Za-z]*\b`)},
	{"a snake_case identifier", regexp.MustCompile(`\b[a-z]+_[a-z_]+\b`)},
}

// domainNouns are terms the UI already teaches the operator, and on-disk names
// they will see with their own eyes.
var domainNouns = `\bGLCB\b|\bRaft\b|\bWAL\b|\bTLS\b|\.corrupt\b`

// configKeys are settings the operator types. Naming one in an alarm is the
// point — it says exactly which knob to turn — so the camelCase and snake_case
// rules do not apply to them. Add a key here only when it is a real config
// field name or CLI flag, never to quiet a leaked internal identifier.
var configKeys = `\bmaxAge\b|\bmaxSize\b|\bmaxChunks\b|\bretention_rules\b|\brefuse=true\b|\bdiskFreeWarn\b|\bdiskFreeFloor\b`

var prose = regexp.MustCompile(domainNouns + `|` + configKeys)

func TestCatalogTextCarriesNoSourceNotation(t *testing.T) {
	t.Parallel()
	for _, at := range Types() {
		for _, f := range []struct{ label, text string }{
			{"Cause", at.Cause},
			{"Response", at.Response},
		} {
			stripped := prose.ReplaceAllString(f.text, "")
			for _, pat := range sourceNotation {
				if m := pat.re.FindString(stripped); m != "" {
					t.Errorf("%s %s contains %s (%q) — operator text must not carry source notation; see the Alarm register entry in docs/ubiquitous_language.md",
						at.IDPrefix, f.label, pat.name, m)
				}
			}
		}
	}
}

// The detectors above are only worth having if they fire. These are the exact
// strings the chunking entries carried before the register rules existed; a
// refactor that quietly stops matching them makes every test in this file pass
// vacuously.
func TestDetectorsFireOnKnownBadText(t *testing.T) {
	t.Parallel()

	badCause := "A vault is repeatedly releasing never-chunked segments at its retention give-up TTL: records are aging out of the completed-segment registry before chunking ever references them."
	badResponse := "The pipeline is not chunking this vault's segments within the retention TTL. The planner needs min(2, placement) holders before it can chunk a segment; check that segment collection is delivering copies to the vault's homes and that replication is progressing."
	badDetail := "vault 06fpdee16hosp0a1q4ru46u370: shedding never-chunked segments at the 3m0s retention give-up TTL"

	t.Run("min(2, placement) reads as a function call", func(t *testing.T) {
		if !sourceNotation[0].re.MatchString(prose.ReplaceAllString(badResponse, "")) {
			t.Error("the call-shaped detector no longer matches min(2, placement)")
		}
	})

	t.Run("3m0s reads as a padded Go duration", func(t *testing.T) {
		if !sourceNotation[1].re.MatchString(badDetail) {
			t.Error("the padded-duration detector no longer matches 3m0s")
		}
		for _, good := range []string{"3m", "12h30m", "1w2d", "90s"} {
			if sourceNotation[1].re.MatchString(good) {
				t.Errorf("the padded-duration detector wrongly rejects %q", good)
			}
		}
	})

	t.Run("the old Cause never names the loss", func(t *testing.T) {
		loss := regexp.MustCompile(`(?i)\b(lost|losing|loss|discarded|dropped|at risk|never reach|cannot be recovered|can never)\b`)
		if loss.MatchString(badCause) {
			t.Error("the consequence detector accepts the old Cause, which never says records are lost")
		}
	})

}

func TestCatalogEveryTypeCarriesCauseAndResponse(t *testing.T) {
	t.Parallel()
	for _, at := range Types() {
		if strings.TrimSpace(at.Cause) == "" {
			t.Errorf("%s has an empty Cause", at.IDPrefix)
		}
		if strings.TrimSpace(at.Response) == "" {
			t.Errorf("%s has an empty Response", at.IDPrefix)
		}
	}
}

// A Critical alarm means records are being lost or are scheduled to be. Its
// Cause has to say so — an operator triaging a list of standing alarms decides
// what to open from this text, and a Critical that reads like a mechanism
// description loses that race against a High that reads like a problem.
func TestCriticalCauseNamesTheConsequence(t *testing.T) {
	t.Parallel()
	loss := regexp.MustCompile(`(?i)\b(lost|losing|loss|discarded|dropped|at risk|never reach|cannot be recovered|can never)\b`)
	for _, at := range Types() {
		if at.Priority != Critical {
			continue
		}
		if !loss.MatchString(at.Cause) {
			t.Errorf("%s is Critical but its Cause never names the consequence (loss / discard / at risk): %q",
				at.IDPrefix, at.Cause)
		}
	}
}

// The detail↔Response restatement guard lives with the raise sites
// (internal/pipeline/chunking), not here: detail is written by the caller, so
// the catalog alone cannot see the pair that actually duplicated.
