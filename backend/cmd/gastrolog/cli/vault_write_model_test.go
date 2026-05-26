package cli

import (
	"strings"
	"testing"
	"time"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParseWriteModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"", "chunk_append", false},
		{"chunk_append", "chunk_append", false},
		{"sequenced", "sequenced", false},
		{"v2", "", true},
		{"bogus", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseWriteModel(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseWriteModel() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWriteModelDisplay(t *testing.T) {
	t.Parallel()

	if got := writeModelDisplay(""); got != "chunk_append (default)" {
		t.Fatalf("empty = %q", got)
	}
	if got := writeModelDisplay("sequenced"); got != "sequenced" {
		t.Fatalf("sequenced = %q", got)
	}
}

func TestVaultDetailPairsIncludesWriteModel(t *testing.T) {
	t.Parallel()

	pairs := vaultDetailPairs(&v1.VaultConfig{
		Name:       "logs",
		WriteModel: "sequenced",
	})
	found := false
	for _, p := range pairs {
		if p[0] == "Write Model" && p[1] == "sequenced" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("vaultDetailPairs missing write model: %#v", pairs)
	}
}

func TestFormatSequencedWatermarkLines(t *testing.T) {
	t.Parallel()

	lines := formatSequencedWatermarkLines(&v1.GetSequencedVaultDiagnosticsResponse{
		NodeId:                   "node-1",
		SpoolWatermark:           10,
		IngestHighWatermark:      12,
		FenceHighWatermark:       8,
		MaterializationWatermark: 8,
		ConvergenceWatermark:     8,
	})
	if len(lines) != 6 {
		t.Fatalf("expected 6 lines, got %d: %v", len(lines), lines)
	}
	if lines[0] != "  Node: node-1" {
		t.Fatalf("unexpected first line: %q", lines[0])
	}
}

func TestFormatSeqAllocatorLines(t *testing.T) {
	t.Parallel()

	lines := formatSeqAllocatorLines(&v1.SeqAllocatorDiagnostics{
		NextSeq: 20,
		Epoch:   2,
		ActiveSwaths: []*v1.SeqActiveLeaseDiagnostics{{
			HolderId: "node-1", Epoch: 2, RangeStart: 11, RangeEnd: 20,
		}},
		BurnedTails: []*v1.SeqBurnedTailDiagnostics{{
			Start: 8, End: 10, Epoch: 1,
		}},
	})
	text := strings.Join(lines, "\n")
	for _, want := range []string{
		"Next seq: 20",
		"Epoch:    2",
		"holder=node-1 epoch=2 range=11-20",
		"8-10 (epoch 1)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}

func TestFormatFenceLines(t *testing.T) {
	t.Parallel()

	created := timestamppb.New(parseTestTime(t, "2026-05-26T12:00:00Z"))
	lines := formatFenceLines([]*v1.FenceRecordDiagnostics{{
		Id: 1, UpperBoundSeq: 50, PrevBoundSeq: 0, CreatedAt: created,
	}})
	if len(lines) != 1 {
		t.Fatalf("expected 1 fence line, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "F_1 upper=50 prev=0 created=2026-05-26 12:00:00 UTC") {
		t.Fatalf("unexpected fence line: %q", lines[0])
	}
}

func TestFormatPeerSequencedWatermarkLines(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	nodeID := glid.New()
	lines := formatPeerSequencedWatermarkLines(vaultID.String(), []*v1.ClusterNode{{
		Id:   nodeID.ToProto(),
		Name: "node-1",
		Stats: &v1.NodeStats{Vaults: []*v1.VaultStats{{
			Id:                       vaultID.ToProto(),
			IngestHighWatermark:      12,
			SpoolWatermark:           10,
			FenceHighWatermark:       8,
			MaterializationWatermark: 8,
			ConvergenceWatermark:     7,
		}}},
	}}, map[string]string{nodeID.String(): "node-1"})
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %v", lines)
	}
	if !strings.Contains(lines[0], "H=12 S_r=10 F_n=8 M_r=8 C_r=7") {
		t.Fatalf("unexpected line: %q", lines[0])
	}
}

func parseTestTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return ts.UTC()
}
