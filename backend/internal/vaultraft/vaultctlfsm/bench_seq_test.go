package vaultctlfsm

import (
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// BenchmarkSeqLeaseReserve measures the protobuf reserve→burn lease lifecycle
// on the per-vault FSM apply path — the hottest seq-allocator path and the one
// the epic (gastrolog-5lrg7) calls out for a no-regression check. It exercises
// marshal + proto.Unmarshal + dispatch + state mutation end to end. The
// measure-first decision: only wire vtprotobuf if this benchmark shows a
// regression versus the prior hand-rolled binary codec.
func BenchmarkSeqLeaseReserve(b *testing.B) {
	f := New()
	const holder = "bench-holder"
	const epoch = uint64(InitialSeqEpoch)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		reserve, err := MarshalReserveSeqRange(holder, epoch, 64)
		if err != nil {
			b.Fatal(err)
		}
		res := f.Apply(&hraft.Log{Data: reserve})
		grant, ok := res.(SeqLeaseGrant)
		if !ok {
			b.Fatalf("reserve: unexpected response %T (%v)", res, res)
		}
		burn, err := MarshalBurnSeqLeaseTail(holder, epoch, grant.End)
		if err != nil {
			b.Fatal(err)
		}
		if r := f.Apply(&hraft.Log{Data: burn}); r != nil {
			b.Fatalf("burn: %v", r)
		}
	}
}

// BenchmarkSeqLeaseReserveMarshalUnmarshal isolates just the wire cost the
// migration changed: building the reserve command and decoding it back. This
// is the most direct apples-to-apples comparison point against the old
// fixed-layout binary encoder.
func BenchmarkSeqLeaseReserveMarshalUnmarshal(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		data, err := MarshalReserveSeqRange("bench-holder", InitialSeqEpoch, 64)
		if err != nil {
			b.Fatal(err)
		}
		var cmd gastrologv1.VaultCtlCommand
		if err := proto.Unmarshal(data, &cmd); err != nil {
			b.Fatal(err)
		}
		if cmd.GetReserveSeqRange() == nil {
			b.Fatal("decoded wrong command case")
		}
	}
}
