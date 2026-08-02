package chunking

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// These tests pin the operator-facing half of a chunking alarm: the detail
// string the raise site writes. The catalog half (Cause/Response) is pinned in
// internal/alert. See the "Alarm register" entry in docs/ubiquitous_language.md
// for the rules both halves answer to.

// capturingSink records every raise so the detail text can be asserted on.
type capturingSink struct {
	raised []capturedAlarm
}

type capturedAlarm struct {
	typeID   string
	instance string
	detail   string
}

func (s *capturingSink) Raise(typeID, instance, detail string) {
	s.raised = append(s.raised, capturedAlarm{typeID, instance, detail})
}

func (s *capturingSink) Clear(string, string) {}

func (s *capturingSink) only(t *testing.T) capturedAlarm {
	t.Helper()
	if len(s.raised) != 1 {
		t.Fatalf("raised %d alarms, want exactly 1: %+v", len(s.raised), s.raised)
	}
	return s.raised[0]
}

// raiseEveryChunkingAlarm drives each chunking raise site that can be reached
// without a live FSM, returning one sink per alarm type. The ghost-segment
// branch of chunking-build-blocked needs a sealed manifest and is covered by
// build_blocked_alert_test.go instead.
func raiseEveryChunkingAlarm(t *testing.T, name string) map[string]capturedAlarm {
	t.Helper()
	vaultName := func() string { return name }
	out := map[string]capturedAlarm{}

	giveUp := &capturingSink{}
	gv, _ := newLoggedVault(VaultConfig{VaultID: glid.New(), VaultName: vaultName, Alerts: giveUp})
	gv.noteRetentionGiveUp(12, 3*time.Minute)
	out[retentionGiveUpAlarmType] = giveUp.only(t)

	underRep := &capturingSink{}
	uv, _ := newLoggedVault(VaultConfig{VaultID: glid.New(), VaultName: vaultName, Alerts: underRep})
	uv.noteUnderReplicated(4, 5*time.Minute)
	out[underReplicatedAlarmType] = underRep.only(t)

	unplannable := &capturingSink{}
	pv, _ := newLoggedVault(VaultConfig{VaultID: glid.New(), VaultName: vaultName, Alerts: unplannable})
	segID := glid.New()
	indexErr := errors.New("corrupt header")
	pv.notePlanFailure(segID, "open segment index", indexErr)
	pv.notePlanFailure(segID, "open segment index", indexErr)
	out[unplannableAlarmType] = unplannable.only(t)

	blocked := &capturingSink{}
	bv, _ := newLoggedVault(VaultConfig{VaultID: glid.New(), VaultName: vaultName, Alerts: blocked})
	bv.noteBuildBlocked(chunk.ChunkID(glid.New()), []glid.GLID{glid.New(), glid.New()})
	out[buildBlockedAlarmType] = blocked.only(t)

	corrupt := &capturingSink{}
	cv, _ := newLoggedVault(VaultConfig{VaultID: glid.New(), VaultName: vaultName, Alerts: corrupt})
	cv.corruptGLCBs = map[chunk.ChunkID]string{chunk.ChunkID(glid.New()): "short read"}
	cv.corruptMu.Lock()
	cv.updateCorruptGLCBAlertLocked()
	cv.corruptMu.Unlock()
	out[glcbCorruptAlarmType] = corrupt.only(t)

	return out
}

// The defect that started this: every chunking alarm announced a bare 26-char
// GLID, which an operator cannot match to anything they configured.
func TestAlarmDetailNamesTheVaultNotItsID(t *testing.T) {
	t.Parallel()
	const name = "app-logs"
	for typeID, got := range raiseEveryChunkingAlarm(t, name) {
		if !strings.Contains(got.detail, `"`+name+`"`) {
			t.Errorf("%s detail does not name the vault %q: %q", typeID, name, got.detail)
		}
		// The instance key stays the ID — it is the dedup key, not prose.
		if strings.Contains(got.detail, got.instance) {
			t.Errorf("%s detail still carries the raw vault ID %s: %q", typeID, got.instance, got.detail)
		}
	}
}

// A vault that has left this node's registry still has to be nameable in the
// alarm that outlives it, so an unresolvable name degrades to the ID rather
// than to an empty pair of quotes.
func TestAlarmDetailFallsBackToVaultIDWhenNameIsUnavailable(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	for _, tc := range []struct {
		name    string
		resolve func() string
	}{
		{"no resolver wired", nil},
		{"vault left the registry", func() string { return "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sink := &capturingSink{}
			v, _ := newLoggedVault(VaultConfig{VaultID: vaultID, VaultName: tc.resolve, Alerts: sink})
			v.noteRetentionGiveUp(1, 3*time.Minute)
			got := sink.only(t)
			if !strings.Contains(got.detail, vaultID.String()) {
				t.Errorf("detail does not fall back to the vault ID: %q", got.detail)
			}
			if strings.Contains(got.detail, `""`) {
				t.Errorf("detail names the vault as an empty string: %q", got.detail)
			}
		})
	}
}

// A rename must reach the next alarm. Resolving the name on each raise rather
// than capturing it at registration is what makes that true.
func TestAlarmDetailFollowsAVaultRename(t *testing.T) {
	t.Parallel()
	name := "app-logs"
	sink := &capturingSink{}
	v, _ := newLoggedVault(VaultConfig{
		VaultID:   glid.New(),
		VaultName: func() string { return name },
		Alerts:    sink,
	})

	v.noteBuildBlocked(chunk.ChunkID(glid.New()), []glid.GLID{glid.New()})
	name = "application-logs"
	v.noteBuildBlocked(chunk.ChunkID(glid.New()), []glid.GLID{glid.New()})

	if len(sink.raised) != 2 {
		t.Fatalf("raised %d alarms, want 2", len(sink.raised))
	}
	if !strings.Contains(sink.raised[1].detail, `"application-logs"`) {
		t.Errorf("second alarm did not pick up the rename: %q", sink.raised[1].detail)
	}
}

var (
	// A padded Go duration: 3m0s, 12h0m0s, 1h0m30s.
	paddedGoDuration = regexp.MustCompile(`\d+[hms]0[ms]`)
	// A call-shaped token — min(2, placement) and friends.
	callShaped = regexp.MustCompile(`\b[a-z][A-Za-z0-9_]*\([^)]*\)`)
)

func TestAlarmDetailCarriesNoSourceNotation(t *testing.T) {
	t.Parallel()
	for typeID, got := range raiseEveryChunkingAlarm(t, "app-logs") {
		// "segment(s)" is prose pluralization, not a call.
		stripped := strings.ReplaceAll(got.detail, "(s)", "s")
		if m := callShaped.FindString(stripped); m != "" {
			t.Errorf("%s detail contains source notation %q: %q", typeID, m, got.detail)
		}
		if m := paddedGoDuration.FindString(got.detail); m != "" {
			t.Errorf("%s detail spells a duration the Go way (%q) instead of via system.FormatDuration: %q",
				typeID, m, got.detail)
		}
	}
}

// The alarm panel shows detail and the catalog Response together. When they
// share a long run of words the operator reads the same sentence twice, which
// is what made the give-up alarm two paragraphs of nothing.
//
// maxRun sits between measurement and the defect: the current entries share at
// most 3 words with their Response, and the give-up text this replaced shared 6
// ("the planner needs min(2, placement) holders").
func TestAlarmDetailDoesNotRestateTheCatalogResponse(t *testing.T) {
	t.Parallel()
	const maxRun = 4
	for typeID, got := range raiseEveryChunkingAlarm(t, "app-logs") {
		at, ok := alert.TypeByID(typeID)
		if !ok {
			t.Fatalf("%s is not in the alarm catalog", typeID)
		}
		shared := longestSharedRun(words(got.detail), words(at.Response))
		if len(shared) > maxRun {
			t.Errorf("%s detail and catalog Response share a %d-word run (%q) — detail says what happened, Response says what to do",
				typeID, len(shared), strings.Join(shared, " "))
		}
	}
}

// Guards the guard: the run detector has to fire on the text that motivated it.
func TestRestatementDetectorFiresOnTheOldGiveUpText(t *testing.T) {
	t.Parallel()
	oldDetail := "vault 06fpdee16hosp0a1q4ru46u370: shedding never-chunked segments at the 3m0s retention give-up TTL — records are dropped before chunking references them; the planner needs min(2, placement) holders and collection is not delivering them"
	oldResponse := "The pipeline is not chunking this vault's segments within the retention TTL. The planner needs min(2, placement) holders before it can chunk a segment; check that segment collection is delivering copies to the vault's homes and that replication is progressing. Clears once the vault seals a chunk again."

	shared := longestSharedRun(words(oldDetail), words(oldResponse))
	if len(shared) <= 4 {
		t.Fatalf("detector found only a %d-word overlap (%q) in text that plainly duplicates itself", len(shared), strings.Join(shared, " "))
	}
	if m := paddedGoDuration.FindString(oldDetail); m == "" {
		t.Error("the padded-duration detector no longer matches 3m0s")
	}
	if m := callShaped.FindString(oldDetail); m == "" {
		t.Error("the call-shape detector no longer matches min(2, placement)")
	}
}

func words(s string) []string { return strings.Fields(strings.ToLower(s)) }

// longestSharedRun returns the longest run of consecutive words present in both
// slices.
func longestSharedRun(a, b []string) []string {
	end, best := 0, 0
	// lengths[j] holds the run ending at a[i-1] and b[j-1] for the current i.
	lengths := make([]int, len(b)+1)
	for i := 1; i <= len(a); i++ {
		prevDiag := 0
		for j := 1; j <= len(b); j++ {
			cur := lengths[j]
			if a[i-1] == b[j-1] {
				lengths[j] = prevDiag + 1
				if lengths[j] > best {
					best, end = lengths[j], i
				}
			} else {
				lengths[j] = 0
			}
			prevDiag = cur
		}
	}
	if best == 0 {
		return nil
	}
	return a[end-best : end]
}
