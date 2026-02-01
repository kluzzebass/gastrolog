package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/index"
)

// fakeChunkManager implements chunk.ChunkManager for testing.
type fakeChunkManager struct{}

func (f *fakeChunkManager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, nil
}
func (f *fakeChunkManager) Seal() error              { return nil }
func (f *fakeChunkManager) Active() *chunk.ChunkMeta { return nil }
func (f *fakeChunkManager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	return chunk.ChunkMeta{}, nil
}
func (f *fakeChunkManager) List() ([]chunk.ChunkMeta, error)                        { return nil, nil }
func (f *fakeChunkManager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) { return nil, nil }
func (f *fakeChunkManager) FindStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, nil
}

// fakeIndexManager implements index.IndexManager for testing.
type fakeIndexManager struct{}

func (f *fakeIndexManager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	return nil
}
func (f *fakeIndexManager) OpenTimeIndex(chunkID chunk.ChunkID) (*index.Index[index.TimeIndexEntry], error) {
	return nil, nil
}
func (f *fakeIndexManager) OpenTokenIndex(chunkID chunk.ChunkID) (*index.Index[index.TokenIndexEntry], error) {
	return nil, nil
}
func (f *fakeIndexManager) OpenAttrKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKeyIndexEntry], error) {
	return nil, nil
}
func (f *fakeIndexManager) OpenAttrValueIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrValueIndexEntry], error) {
	return nil, nil
}
func (f *fakeIndexManager) OpenAttrKVIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKVIndexEntry], error) {
	return nil, nil
}
func (f *fakeIndexManager) IndexesComplete(chunkID chunk.ChunkID) (bool, error) {
	return true, nil
}

// fakeReceiver implements Receiver for testing.
type fakeReceiver struct{}

func (f *fakeReceiver) Run(ctx context.Context, out chan<- IngestMessage) error {
	<-ctx.Done()
	return nil
}

func TestApplyConfigNil(t *testing.T) {
	orch := New(Config{})
	err := orch.ApplyConfig(nil, Factories{})
	if err != nil {
		t.Errorf("expected nil error for nil config, got %v", err)
	}
}

func TestApplyConfigStores(t *testing.T) {
	orch := New(Config{})

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				return &fakeIndexManager{}, nil
			},
		},
	}

	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "store1", Type: "memory", Params: map[string]string{}},
			{ID: "store2", Type: "memory", Params: map[string]string{}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify stores were registered.
	if len(orch.chunks) != 2 {
		t.Errorf("expected 2 chunk managers, got %d", len(orch.chunks))
	}
	if len(orch.indexes) != 2 {
		t.Errorf("expected 2 index managers, got %d", len(orch.indexes))
	}
	if len(orch.queries) != 2 {
		t.Errorf("expected 2 query engines, got %d", len(orch.queries))
	}
}

func TestApplyConfigReceivers(t *testing.T) {
	orch := New(Config{})

	factories := Factories{
		Receivers: map[string]ReceiverFactory{
			"test": func(params map[string]string, logger *slog.Logger) (Receiver, error) {
				return &fakeReceiver{}, nil
			},
		},
	}

	cfg := &config.Config{
		Receivers: []config.ReceiverConfig{
			{ID: "recv1", Type: "test", Params: map[string]string{}},
			{ID: "recv2", Type: "test", Params: map[string]string{}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(orch.receivers) != 2 {
		t.Errorf("expected 2 receivers, got %d", len(orch.receivers))
	}
}

func TestApplyConfigUnknownChunkManagerType(t *testing.T) {
	orch := New(Config{})

	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "store1", Type: "unknown", Params: map[string]string{}},
		},
	}

	err := orch.ApplyConfig(cfg, Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{},
		IndexManagers: map[string]index.ManagerFactory{},
	})
	if err == nil {
		t.Error("expected error for unknown chunk manager type")
	}
}

func TestApplyConfigUnknownIndexManagerType(t *testing.T) {
	orch := New(Config{})

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{}, // missing "memory"
	}

	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "store1", Type: "memory", Params: map[string]string{}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err == nil {
		t.Error("expected error for unknown index manager type")
	}
}

func TestApplyConfigUnknownReceiverType(t *testing.T) {
	orch := New(Config{})

	cfg := &config.Config{
		Receivers: []config.ReceiverConfig{
			{ID: "recv1", Type: "unknown", Params: map[string]string{}},
		},
	}

	err := orch.ApplyConfig(cfg, Factories{
		Receivers: map[string]ReceiverFactory{},
	})
	if err == nil {
		t.Error("expected error for unknown receiver type")
	}
}

func TestApplyConfigDuplicateStoreID(t *testing.T) {
	orch := New(Config{})

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				return &fakeIndexManager{}, nil
			},
		},
	}

	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "store1", Type: "memory", Params: map[string]string{}},
			{ID: "store1", Type: "memory", Params: map[string]string{}}, // duplicate
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err == nil {
		t.Error("expected error for duplicate store ID")
	}
}

func TestApplyConfigDuplicateReceiverID(t *testing.T) {
	orch := New(Config{})

	factories := Factories{
		Receivers: map[string]ReceiverFactory{
			"test": func(params map[string]string, logger *slog.Logger) (Receiver, error) {
				return &fakeReceiver{}, nil
			},
		},
	}

	cfg := &config.Config{
		Receivers: []config.ReceiverConfig{
			{ID: "recv1", Type: "test", Params: map[string]string{}},
			{ID: "recv1", Type: "test", Params: map[string]string{}}, // duplicate
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err == nil {
		t.Error("expected error for duplicate receiver ID")
	}
}

func TestApplyConfigChunkManagerFactoryError(t *testing.T) {
	orch := New(Config{})

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string) (chunk.ChunkManager, error) {
				return nil, errors.New("factory error")
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				return &fakeIndexManager{}, nil
			},
		},
	}

	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "store1", Type: "memory", Params: map[string]string{}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err == nil {
		t.Error("expected error from chunk manager factory")
	}
}

func TestApplyConfigIndexManagerFactoryError(t *testing.T) {
	orch := New(Config{})

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				return nil, errors.New("factory error")
			},
		},
	}

	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "store1", Type: "memory", Params: map[string]string{}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err == nil {
		t.Error("expected error from index manager factory")
	}
}

func TestApplyConfigReceiverFactoryError(t *testing.T) {
	orch := New(Config{})

	factories := Factories{
		Receivers: map[string]ReceiverFactory{
			"test": func(params map[string]string, logger *slog.Logger) (Receiver, error) {
				return nil, errors.New("factory error")
			},
		},
	}

	cfg := &config.Config{
		Receivers: []config.ReceiverConfig{
			{ID: "recv1", Type: "test", Params: map[string]string{}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err == nil {
		t.Error("expected error from receiver factory")
	}
}

func TestApplyConfigParamsPassedToReceiverFactory(t *testing.T) {
	orch := New(Config{})

	var receivedParams map[string]string
	factories := Factories{
		Receivers: map[string]ReceiverFactory{
			"test": func(params map[string]string, logger *slog.Logger) (Receiver, error) {
				receivedParams = params
				return &fakeReceiver{}, nil
			},
		},
	}

	cfg := &config.Config{
		Receivers: []config.ReceiverConfig{
			{ID: "recv1", Type: "test", Params: map[string]string{
				"host": "localhost",
				"port": "514",
			}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedParams["host"] != "localhost" {
		t.Errorf("expected host=localhost, got %s", receivedParams["host"])
	}
	if receivedParams["port"] != "514" {
		t.Errorf("expected port=514, got %s", receivedParams["port"])
	}
}

func TestApplyConfigParamsPassedToStoreFactories(t *testing.T) {
	orch := New(Config{})

	var cmReceivedParams map[string]string
	var imReceivedParams map[string]string

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"test": func(params map[string]string) (chunk.ChunkManager, error) {
				cmReceivedParams = params
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"test": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				imReceivedParams = params
				return &fakeIndexManager{}, nil
			},
		},
	}

	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "store1", Type: "test", Params: map[string]string{
				"dir":          "/data/chunks",
				"timeSparsity": "500",
			}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify params passed to chunk manager factory.
	if cmReceivedParams["dir"] != "/data/chunks" {
		t.Errorf("chunk manager: expected dir=/data/chunks, got %s", cmReceivedParams["dir"])
	}
	if cmReceivedParams["timeSparsity"] != "500" {
		t.Errorf("chunk manager: expected timeSparsity=500, got %s", cmReceivedParams["timeSparsity"])
	}

	// Verify params passed to index manager factory.
	if imReceivedParams["dir"] != "/data/chunks" {
		t.Errorf("index manager: expected dir=/data/chunks, got %s", imReceivedParams["dir"])
	}
	if imReceivedParams["timeSparsity"] != "500" {
		t.Errorf("index manager: expected timeSparsity=500, got %s", imReceivedParams["timeSparsity"])
	}
}

func TestApplyConfigIndexManagerReceivesChunkManager(t *testing.T) {
	orch := New(Config{})

	expectedCM := &fakeChunkManager{}
	var receivedCM chunk.ChunkManager

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"test": func(params map[string]string) (chunk.ChunkManager, error) {
				return expectedCM, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"test": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				receivedCM = cm
				return &fakeIndexManager{}, nil
			},
		},
	}

	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "store1", Type: "test", Params: map[string]string{}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedCM != expectedCM {
		t.Error("index manager factory did not receive the correct chunk manager")
	}
}
