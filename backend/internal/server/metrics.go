package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"gastrolog/internal/orchestrator"
)

// registerMetrics registers the /metrics endpoint for Prometheus scraping.
// This endpoint is unauthenticated (standard for Prometheus targets).
func (s *Server) registerMetrics(mux *http.ServeMux) {
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		s.writeMetrics(w)
	})
}

func (s *Server) writeMetrics(w http.ResponseWriter) {
	orch := s.orch

	// -- Server info --
	_, _ = fmt.Fprintf(w, "# HELP gastrolog_info Server version and metadata.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_info gauge\n")
	_, _ = fmt.Fprintf(w, "gastrolog_info{version=%q} 1\n", Version)

	_, _ = fmt.Fprintf(w, "# HELP gastrolog_up Whether the server is running.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_up gauge\n")
	if orch.IsRunning() {
		_, _ = fmt.Fprintf(w, "gastrolog_up 1\n")
	} else {
		_, _ = fmt.Fprintf(w, "gastrolog_up 0\n")
	}

	_, _ = fmt.Fprintf(w, "# HELP gastrolog_uptime_seconds Seconds since server start.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_uptime_seconds gauge\n")
	_, _ = fmt.Fprintf(w, "gastrolog_uptime_seconds %.0f\n", time.Since(s.startTime).Seconds())

	// -- Ingest queue --
	_, _ = fmt.Fprintf(w, "# HELP gastrolog_ingest_queue_depth Current messages in the ingest queue.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_ingest_queue_depth gauge\n")
	_, _ = fmt.Fprintf(w, "gastrolog_ingest_queue_depth %d\n", orch.IngestQueueDepth())

	_, _ = fmt.Fprintf(w, "# HELP gastrolog_ingest_queue_capacity Total capacity of the ingest queue.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_ingest_queue_capacity gauge\n")
	_, _ = fmt.Fprintf(w, "gastrolog_ingest_queue_capacity %d\n", orch.IngestQueueCapacity())

	// -- Per-ingester metrics --
	s.writeIngesterMetrics(w, orch)

	// -- Vault metrics --
	s.writeVaultMetrics(w, orch)
}

func (s *Server) writeIngesterMetrics(w http.ResponseWriter, orch *orchestrator.Orchestrator) {
	ids := orch.ListIngesters()
	if len(ids) == 0 {
		return
	}

	// Build name lookup from config.
	nameMap := make(map[string]string, len(ids))
	if s.cfgStore != nil {
		ingesters, err := s.cfgStore.ListIngesters(context.Background())
		if err == nil {
			for _, ing := range ingesters {
				nameMap[ing.ID.String()] = ing.Name
			}
		}
	}

	_, _ = fmt.Fprintf(w, "# HELP gastrolog_ingester_messages_total Total messages ingested.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_ingester_messages_total counter\n")
	for _, id := range ids {
		idStr := id.String()
		name := nameMap[idStr]
		stats := orch.GetIngesterStats(id)
		if stats == nil {
			continue
		}
		_, _ = fmt.Fprintf(w, "gastrolog_ingester_messages_total{ingester=%q,name=%q} %d\n",
			idStr, name, stats.MessagesIngested.Load())
	}

	_, _ = fmt.Fprintf(w, "# HELP gastrolog_ingester_bytes_total Total bytes ingested.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_ingester_bytes_total counter\n")
	for _, id := range ids {
		idStr := id.String()
		name := nameMap[idStr]
		stats := orch.GetIngesterStats(id)
		if stats == nil {
			continue
		}
		_, _ = fmt.Fprintf(w, "gastrolog_ingester_bytes_total{ingester=%q,name=%q} %d\n",
			idStr, name, stats.BytesIngested.Load())
	}

	_, _ = fmt.Fprintf(w, "# HELP gastrolog_ingester_errors_total Total ingestion errors.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_ingester_errors_total counter\n")
	for _, id := range ids {
		idStr := id.String()
		name := nameMap[idStr]
		stats := orch.GetIngesterStats(id)
		if stats == nil {
			continue
		}
		_, _ = fmt.Fprintf(w, "gastrolog_ingester_errors_total{ingester=%q,name=%q} %d\n",
			idStr, name, stats.Errors.Load())
	}
}

func (s *Server) writeVaultMetrics(w http.ResponseWriter, orch *orchestrator.Orchestrator) {
	vaults := orch.ListVaults()
	if len(vaults) == 0 {
		return
	}

	// Build name/type lookup from config.
	type vaultMeta struct {
		name      string
		vaultType string
	}
	metaMap := make(map[string]vaultMeta, len(vaults))
	if s.cfgStore != nil {
		cfgStores, err := s.cfgStore.ListVaults(context.Background())
		if err == nil {
			for _, st := range cfgStores {
				metaMap[st.ID.String()] = vaultMeta{name: st.Name, vaultType: st.Type}
			}
		}
	}

	_, _ = fmt.Fprintf(w, "# HELP gastrolog_store_chunks_total Total chunks per vault.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_store_chunks_total gauge\n")
	_, _ = fmt.Fprintf(w, "# HELP gastrolog_store_chunks_sealed Sealed chunks per vault.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_store_chunks_sealed gauge\n")
	_, _ = fmt.Fprintf(w, "# HELP gastrolog_store_records_total Total records per vault.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_store_records_total gauge\n")
	_, _ = fmt.Fprintf(w, "# HELP gastrolog_store_bytes Total data bytes per vault.\n")
	_, _ = fmt.Fprintf(w, "# TYPE gastrolog_store_bytes gauge\n")

	for _, vaultID := range vaults {
		idStr := vaultID.String()
		meta := metaMap[idStr]

		metas, err := orch.ListChunkMetas(vaultID)
		if err != nil {
			continue
		}

		var sealed int64
		var records int64
		var dataBytes int64
		for _, m := range metas {
			if m.Sealed {
				sealed++
			}
			records += m.RecordCount
			if m.DiskBytes > 0 {
				dataBytes += m.DiskBytes
			} else {
				dataBytes += m.Bytes
			}
		}

		labels := fmt.Sprintf("vault=%q,name=%q,type=%q", idStr, meta.name, meta.vaultType)
		_, _ = fmt.Fprintf(w, "gastrolog_store_chunks_total{%s} %d\n", labels, len(metas))
		_, _ = fmt.Fprintf(w, "gastrolog_store_chunks_sealed{%s} %d\n", labels, sealed)
		_, _ = fmt.Fprintf(w, "gastrolog_store_records_total{%s} %d\n", labels, records)
		_, _ = fmt.Fprintf(w, "gastrolog_store_bytes{%s} %d\n", labels, dataBytes)
	}
}
