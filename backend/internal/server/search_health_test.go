package server

import (
	"context"
	"errors"
	"testing"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
)

func standing(c *alert.Collector, typeID, key string) bool {
	for _, a := range c.Standing() {
		if a.TypeID == typeID && a.InstanceKey == key {
			return true
		}
	}
	return false
}

// A read failure raises the alarm for the vault it names; a later successful
// search over that vault clears it; cancellation and other errors touch nothing.
func TestNoteSearchOutcomeRaisesOnReadFailureAndClearsOnSuccess(t *testing.T) {
	alerts := alert.New()
	v := glid.New()
	readErr := &query.VaultReadError{VaultID: v, ChunkID: chunk.NewChunkID(), Err: errors.New("mmap: bad file")}

	NoteSearchOutcome(alerts, readErr, nil)
	if !standing(alerts, vaultSearchFailingAlarm, v.String()) {
		t.Fatal("read failure did not raise the vault alarm")
	}

	NoteSearchOutcome(alerts, context.Canceled, func() []glid.GLID { return []glid.GLID{v} })
	NoteSearchOutcome(alerts, errors.New("parse error"), func() []glid.GLID { return []glid.GLID{v} })
	if !standing(alerts, vaultSearchFailingAlarm, v.String()) {
		t.Fatal("an unrelated error cleared the alarm")
	}

	NoteSearchOutcome(alerts, nil, func() []glid.GLID { return []glid.GLID{v} })
	if standing(alerts, vaultSearchFailingAlarm, v.String()) {
		t.Fatal("a successful search over the vault did not clear the alarm")
	}
}

// The forwarded-search wrapper reports the outcome only once the stream has
// been consumed, on the node whose copy was read.
func TestObserveSearchIteratorReportsAfterTheStreamEnds(t *testing.T) {
	alerts := alert.New()
	v := glid.New()
	failing := func(yield func(chunk.Record, error) bool) {
		if !yield(chunk.Record{Raw: []byte("ok")}, nil) {
			return
		}
		yield(chunk.Record{}, &query.VaultReadError{VaultID: v, ChunkID: chunk.NewChunkID(), Err: errors.New("read")})
	}
	for range ObserveSearchIterator(alerts, v, failing) {
	}
	if !standing(alerts, vaultSearchFailingAlarm, v.String()) {
		t.Fatal("read failure mid-stream did not raise the alarm")
	}

	clean := func(yield func(chunk.Record, error) bool) { yield(chunk.Record{Raw: []byte("ok")}, nil) }
	for range ObserveSearchIterator(alerts, v, clean) {
	}
	if standing(alerts, vaultSearchFailingAlarm, v.String()) {
		t.Fatal("a clean stream did not clear the alarm")
	}
}
