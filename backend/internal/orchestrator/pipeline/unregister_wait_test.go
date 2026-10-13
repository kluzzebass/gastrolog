package pipeline

import (
	"context"
	"io"
	"testing"
	"testing/synctest"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
)

// assignedLog assigns one segment to the vault on every roll.
type assignedLog struct {
	seg collection.AssignedSegment
}

func (l assignedLog) Roll(context.Context, glid.GLID) ([]collection.AssignedSegment, error) {
	return []collection.AssignedSegment{l.seg}, nil
}

// parkedPull parks the first pull, ignoring its context like a write already
// under way, until released.
type parkedPull struct {
	parked  chan struct{}
	release chan struct{}
	first   bool
}

func (p *parkedPull) Pull(context.Context, glid.GLID, glid.GLID, []string, io.Writer) (string, error) {
	if !p.first {
		p.first = true
		close(p.parked)
		<-p.release
	}
	return "", io.ErrUnexpectedEOF
}

// A vault's files are removed after the supervisor's UnregisterVault wait
// returns, so the wait must cover every goroutine still writing under the
// vault's root — here a home's collection pull.
func TestSupervisorUnregisterVaultWaitCoversInFlightWork(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		vaultID := glid.New()
		sup := New(Config{NodeID: glid.New()})
		if err := sup.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = sup.Stop() }()

		pull := &parkedPull{parked: make(chan struct{}), release: make(chan struct{})}
		if err := sup.RegisterVault(VaultSpec{
			VaultID:  vaultID,
			Home:     true,
			HomeRoot: t.TempDir(),
			Log:      assignedLog{seg: collection.AssignedSegment{VaultID: vaultID, SegmentID: glid.New()}},
			Pull:     pull,
			Receipts: &countingReceipts{},
		}); err != nil {
			t.Fatalf("RegisterVault: %v", err)
		}
		<-pull.parked

		wait := sup.UnregisterVault(vaultID)
		waited := make(chan struct{})
		go func() {
			wait()
			close(waited)
		}()
		synctest.Wait()
		select {
		case <-waited:
			t.Fatal("UnregisterVault's wait returned while a pull was still writing under the vault's root")
		default:
		}
		close(pull.release)
		<-waited
	})
}
