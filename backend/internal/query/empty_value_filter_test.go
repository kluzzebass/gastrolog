package query

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memjson "gastrolog/internal/index/memory/json"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/querylang"
)

// An empty string is a value like any other: key="" matches records whose
// key holds the empty string, key!="" matches records whose key holds
// anything else, and only key=* asks for the key's presence.
func TestEmptyValueIsAValueNotKeyExists(t *testing.T) {
	cm, err := chunkmem.NewManager(chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(1000)})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i, attrs := range []chunk.Attributes{
		{"note": "a b"},
		{"note": ""},
		{"other": "v"},
	} {
		ts := t0.Add(time.Duration(i) * time.Second)
		cm.Append(chunk.Record{IngestTS: ts, WriteTS: ts, Raw: []byte("line"), Attrs: attrs})
	}
	cm.Seal()
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	jsonIdx := memjson.NewIndexer(cm)
	im := indexmem.NewManagerWithJSON([]index.Indexer{tokIdx, attrIdx, kvIdx, jsonIdx}, tokIdx, attrIdx, kvIdx, jsonIdx, nil)
	metas, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range metas {
		if err := im.BuildIndexes(context.Background(), m.ID); err != nil {
			t.Fatalf("build indexes: %v", err)
		}
	}
	eng := New(cm, im, nil)

	tests := []struct {
		expr string
		want int
	}{
		{`note=""`, 1},
		{`note!=""`, 1},
		{`note=*`, 2},
		{`note="a b"`, 1},
		{`NOT note=""`, 2},
	}
	for _, tc := range tests {
		expr, err := querylang.Parse(tc.expr)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.expr, err)
		}
		it, _ := eng.Search(context.Background(), Query{BoolExpr: expr}, nil)
		got := 0
		for _, err := range it {
			if err != nil {
				t.Fatalf("%s: %v", tc.expr, err)
			}
			got++
		}
		if got != tc.want {
			t.Errorf("%s matched %d records, want %d", tc.expr, got, tc.want)
		}
	}
}
