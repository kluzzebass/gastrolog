package server

import (
	"errors"
	"fmt"
	"iter"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
)

// vaultSearchFailingAlarm stands while searches fail because a vault's bytes
// cannot be read on this node, keyed by vault.
const vaultSearchFailingAlarm = "vault-search-failing"

// NoteSearchOutcome raises the standing alarm when a search failed reading a
// vault's chunk on this node, and clears it for every vault a successful
// search covered. Cancellation and query-shape errors say nothing about the
// vault and leave the alarms alone. searched is consulted only on success.
func NoteSearchOutcome(alerts alert.Sink, err error, searched func() []glid.GLID) {
	if alerts == nil {
		return
	}
	if re, ok := errors.AsType[*query.VaultReadError](err); ok {
		if !re.VaultID.IsZero() {
			alerts.Raise(vaultSearchFailingAlarm, re.VaultID.String(),
				fmt.Sprintf("search failed reading chunk %s: %v", re.ChunkID, re.Err))
		}
		return
	}
	if err != nil || searched == nil {
		return
	}
	for _, v := range searched() {
		alerts.Clear(vaultSearchFailingAlarm, v.String())
	}
}

// ObserveSearchIterator reports a forwarded search's outcome once the stream
// is consumed: a read failure raises the alarm on this node, whose copy of
// the vault is the one that failed; a clean end clears it.
func ObserveSearchIterator(alerts alert.Sink, vaultID glid.GLID, it iter.Seq2[chunk.Record, error]) iter.Seq2[chunk.Record, error] {
	if alerts == nil || it == nil {
		return it
	}
	return func(yield func(chunk.Record, error) bool) {
		for rec, err := range it {
			if err != nil {
				NoteSearchOutcome(alerts, err, nil)
				yield(rec, err)
				return
			}
			if !yield(rec, nil) {
				return
			}
		}
		NoteSearchOutcome(alerts, nil, func() []glid.GLID { return []glid.GLID{vaultID} })
	}
}
