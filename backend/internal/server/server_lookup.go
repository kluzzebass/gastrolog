package server

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/config"
	"gastrolog/internal/home"
	"gastrolog/internal/lookup"
)

// loadInitialLookupConfig loads MMDB, HTTP, and JSON lookup tables from persisted config at startup.
func (s *Server) loadInitialLookupConfig(registry lookup.Registry) {
	if s.cfgStore == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
	defer cancel()
	ss, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		s.logger.Warn("failed to load lookup config at startup", "error", err)
		return
	}
	s.applyLookupConfig(ss.Lookup, ss.MaxMind, registry)
}

// resolveMMDBPath finds an MMDB file via the managed file manifest.
func (s *Server) resolveMMDBPath(ctx context.Context, filename string) string {
	return s.ResolveManagedFilePath(ctx, filename)
}

// applyLookupConfig loads (or reloads) MMDB, HTTP, and JSON lookup tables from the given config.
// It also manages the maxmind-update cron job for automatic downloads.
func (s *Server) applyLookupConfig(cfg config.LookupConfig, mm config.MaxMindConfig, registry lookup.Registry) {
	// Register MMDB lookup tables (GeoIP City / ASN) from config.
	s.registerMMDBLookups(cfg, registry)

	// Register HTTP lookup tables from config.
	s.registerHTTPLookups(cfg, registry)

	// Register JSON file lookup tables from config.
	s.registerJSONFileLookups(cfg, registry)

	// Register CSV lookup tables from config.
	s.registerCSVLookups(cfg, registry)

	// Manage the maxmind-update cron job.
	s.manageMaxMindJob(mm, registry)
}

// registerMMDBLookups registers MMDB-backed lookup tables (GeoIP City / ASN) from config.
// Follows the same lifecycle pattern as registerJSONFileLookups: close+remove stale, create+load new.
func (s *Server) registerMMDBLookups(cfg config.LookupConfig, registry lookup.Registry) {
	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
	defer cancel()

	// Build keep set of names that should exist.
	keep := make(map[string]struct{}, len(cfg.MMDBLookups))
	for _, mcfg := range cfg.MMDBLookups {
		if mcfg.Name != "" {
			keep[mcfg.Name] = struct{}{}
		}
	}

	// Close and remove any MMDB lookups no longer in config.
	for name, table := range registry {
		if m, ok := table.(*lookup.MMDB); ok {
			if _, exists := keep[name]; !exists {
				m.Close()
				delete(registry, name)
				s.logger.Info("removed MMDB lookup table", "name", name)
			}
		}
	}

	for _, mcfg := range cfg.MMDBLookups {
		if mcfg.Name == "" {
			continue
		}

		// Resolve MMDB path: from managed file ID, or auto-downloaded by db_type.
		var mmdbPath string
		if mcfg.FileID != "" {
			mmdbPath = s.ResolveManagedFileByID(ctx, mcfg.FileID)
		} else {
			// No file_id → use auto-downloaded database by type.
			mmdbPath = s.resolveMMDBPath(ctx, mmdbFileName(mcfg.DBType))
		}
		// Close existing table if any.
		if existing, ok := registry[mcfg.Name]; ok {
			if m, ok := existing.(*lookup.MMDB); ok {
				m.Close()
			}
		}
		m := lookup.NewMMDB(mcfg.DBType)
		if mmdbPath != "" {
			if info, err := m.Load(mmdbPath); err != nil {
				s.logger.Warn("failed to load MMDB", "name", mcfg.Name, "path", mmdbPath, "error", err)
			} else {
				s.logger.Info("loaded MMDB lookup", "name", mcfg.Name, "type", info.DatabaseType, "build", info.BuildTime.Format("2006-01-02"))
				if err := m.WatchFile(mmdbPath); err != nil {
					s.logger.Warn("failed to watch MMDB file", "name", mcfg.Name, "path", mmdbPath, "error", err)
				}
			}
		}
		registry[mcfg.Name] = m
	}
}

// mmdbFileName returns the auto-download filename for a given MMDB db type.
func mmdbFileName(dbType string) string {
	switch dbType {
	case "city":
		return "GeoLite2-City.mmdb"
	case "asn":
		return "GeoLite2-ASN.mmdb"
	default:
		return ""
	}
}

// registerHTTPLookups registers HTTP API lookup tables from config into the registry.
func (s *Server) registerHTTPLookups(cfg config.LookupConfig, registry lookup.Registry) {
	for _, hcfg := range cfg.HTTPLookups {
		if hcfg.Name == "" || hcfg.URLTemplate == "" {
			continue
		}

		paramNames := make([]string, len(hcfg.Parameters))
		for j, p := range hcfg.Parameters {
			paramNames[j] = p.Name
		}
		lcfg := lookup.HTTPConfig{
			URLTemplate:   hcfg.URLTemplate,
			Headers:       hcfg.Headers,
			ResponsePaths: hcfg.ResponsePaths,
			Parameters:    paramNames,
			CacheSize:     hcfg.CacheSize,
		}
		if hcfg.Timeout != "" {
			if d, err := time.ParseDuration(hcfg.Timeout); err == nil {
				lcfg.Timeout = d
			}
		}
		if hcfg.CacheTTL != "" {
			if d, err := time.ParseDuration(hcfg.CacheTTL); err == nil {
				lcfg.CacheTTL = d
			}
		}

		registry[hcfg.Name] = lookup.NewHTTP(lcfg)
		s.logger.Info("registered HTTP lookup table", "name", hcfg.Name, "url", hcfg.URLTemplate)
	}
}

// registerJSONFileLookups registers JSON file-backed lookup tables from config into the registry.
// It also cleans up any previously registered JSON file lookups that are no longer in the config.
func (s *Server) registerJSONFileLookups(cfg config.LookupConfig, registry lookup.Registry) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Build set of names that should exist.
	keep := make(map[string]struct{}, len(cfg.JSONFileLookups))
	for _, jcfg := range cfg.JSONFileLookups {
		if jcfg.Name != "" {
			keep[jcfg.Name] = struct{}{}
		}
	}

	// Close and remove any JSON file lookups no longer in config.
	for name, table := range registry {
		if jf, ok := table.(*lookup.JSONFile); ok {
			if _, exists := keep[name]; !exists {
				jf.Close()
				delete(registry, name)
				s.logger.Info("removed JSON file lookup table", "name", name)
			}
		}
	}

	for _, jcfg := range cfg.JSONFileLookups {
		if jcfg.Name == "" || jcfg.FileID == "" {
			continue
		}

		// Close any existing JSON file lookup with the same name (stops its watcher).
		if existing, ok := registry[jcfg.Name]; ok {
			if jf, ok := existing.(*lookup.JSONFile); ok {
				jf.Close()
			}
		}

		filePath := s.ResolveManagedFileByID(ctx, jcfg.FileID)
		if filePath == "" {
			s.logger.Warn("JSON lookup file not found", "name", jcfg.Name, "file_id", jcfg.FileID)
			continue
		}

		paramNames := make([]string, len(jcfg.Parameters))
		for k, p := range jcfg.Parameters {
			paramNames[k] = p.Name
		}
		jf := lookup.NewJSONFile(lookup.JSONFileConfig{
			Query:         jcfg.Query,
			ResponsePaths: jcfg.ResponsePaths,
			Parameters:    paramNames,
		})

		if err := jf.Load(filePath); err != nil {
			s.logger.Warn("failed to load JSON lookup file", "name", jcfg.Name, "path", filePath, "error", err)
			continue
		}
		if err := jf.WatchFile(filePath); err != nil {
			s.logger.Warn("failed to watch JSON lookup file", "name", jcfg.Name, "path", filePath, "error", err)
		}

		registry[jcfg.Name] = jf
		s.logger.Info("registered JSON file lookup table", "name", jcfg.Name, "path", filePath)
	}
}

// registerCSVLookups registers CSV file-backed lookup tables from config.
func (s *Server) registerCSVLookups(cfg config.LookupConfig, registry lookup.Registry) {
	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
	defer cancel()

	keep := make(map[string]struct{}, len(cfg.CSVLookups))
	for _, ccfg := range cfg.CSVLookups {
		if ccfg.Name != "" {
			keep[ccfg.Name] = struct{}{}
		}
	}

	// Close and remove any CSV lookups no longer in config.
	for name, table := range registry {
		if ct, ok := table.(*lookup.CSV); ok {
			if _, exists := keep[name]; !exists {
				ct.Close()
				delete(registry, name)
				s.logger.Info("removed CSV lookup table", "name", name)
			}
		}
	}

	for _, ccfg := range cfg.CSVLookups {
		if ccfg.Name == "" || ccfg.FileID == "" {
			continue
		}

		if existing, ok := registry[ccfg.Name]; ok {
			if ct, ok := existing.(*lookup.CSV); ok {
				ct.Close()
			}
		}

		filePath := s.ResolveManagedFileByID(ctx, ccfg.FileID)
		if filePath == "" {
			s.logger.Warn("CSV lookup file not found", "name", ccfg.Name, "file_id", ccfg.FileID)
			continue
		}

		ct := lookup.NewCSV(lookup.CSVConfig{
			KeyColumn:    ccfg.KeyColumn,
			ValueColumns: ccfg.ValueColumns,
		})

		if err := ct.Load(filePath); err != nil {
			s.logger.Warn("failed to load CSV lookup file", "name", ccfg.Name, "path", filePath, "error", err)
			continue
		}
		if err := ct.WatchFile(filePath); err != nil {
			s.logger.Warn("failed to watch CSV lookup file", "name", ccfg.Name, "path", filePath, "error", err)
		}

		registry[ccfg.Name] = ct
		s.logger.Info("registered CSV lookup table", "name", ccfg.Name, "path", filePath)
	}
}

// manageMaxMindJob adds or removes the maxmind-update cron job based on config.
func (s *Server) manageMaxMindJob(mm config.MaxMindConfig, registry lookup.Registry) {
	scheduler := s.orch.Scheduler()
	if scheduler == nil {
		return
	}

	hasCredentials := mm.AccountID != "" && mm.LicenseKey != ""
	if !mm.AutoDownload || !hasCredentials || s.homeDir == "" {
		scheduler.RemoveJob("maxmind-update")
		return
	}

	updateFn := func() { s.runMaxMindUpdate(registry) }

	// Add recurring cron job: 03:00 on Tuesdays and Fridays.
	if err := scheduler.AddJob("maxmind-update", "0 3 * * 2,5", updateFn); err != nil {
		// Job may already exist (e.g. config re-applied). Update it.
		if err := scheduler.UpdateJob("maxmind-update", "0 3 * * 2,5", updateFn); err != nil {
			s.logger.Warn("failed to update maxmind-update job", "error", err)
		}
	}
	scheduler.Describe("maxmind-update", "Download MaxMind GeoLite2 databases")

	// If any MMDB entry has no file available yet, trigger an immediate download.
	// Load current lookup config to check MMDB entries.
	loadCtx, loadCancel := context.WithTimeout(context.Background(), configLoadTimeout)
	ss, err := s.cfgStore.LoadServerSettings(loadCtx)
	loadCancel()
	needsDownload := false
	if err == nil {
		for _, mcfg := range ss.Lookup.MMDBLookups {
			if mcfg.FileID == "" {
				needsDownload = true
				break
			}
		}
	}
	if needsDownload {
		_ = scheduler.RunOnce("maxmind-update-initial", updateFn)
	}
}

// runMaxMindUpdate downloads both MaxMind editions, registers them as managed files,
// and reloads any MMDB registry entries that use auto-downloaded databases.
func (s *Server) runMaxMindUpdate(registry lookup.Registry) {
	loadCtx, loadCancel := context.WithTimeout(context.Background(), configLoadTimeout)
	ss, err := s.cfgStore.LoadServerSettings(loadCtx)
	loadCancel()
	if err != nil {
		s.logger.Warn("maxmind update: load config failed", "error", err)
		return
	}

	if !ss.MaxMind.AutoDownload || ss.MaxMind.AccountID == "" || ss.MaxMind.LicenseKey == "" {
		return
	}

	hd := home.New(s.homeDir)
	downloadDir := hd.ManagedFilesDir()
	if err := os.MkdirAll(downloadDir, 0o750); err != nil {
		s.logger.Warn("maxmind update: create download dir", "error", err)
		return
	}

	ctx := context.Background()

	var anySuccess bool
	for _, edition := range []string{"GeoLite2-City", "GeoLite2-ASN"} {
		if err := lookup.DownloadDB(ctx, ss.MaxMind.AccountID, ss.MaxMind.LicenseKey, edition, downloadDir); err != nil {
			s.logger.Warn("maxmind update: download failed", "edition", edition, "error", err)
			continue
		}
		s.logger.Info("maxmind update: downloaded", "edition", edition)
		anySuccess = true

		// Register as a managed file entity so it distributes to all nodes.
		flatPath := filepath.Join(downloadDir, edition+".mmdb")
		if lf, err := s.RegisterFile(ctx, flatPath, ""); err != nil {
			s.logger.Warn("maxmind update: register file failed", "edition", edition, "error", err)
		} else {
			s.logger.Info("maxmind update: registered as managed file", "edition", edition, "file_id", lf.ID)
		}
	}

	if !anySuccess {
		return
	}

	// Re-apply config to reload MMDB entries that use auto-downloaded databases.
	reloadCtx, reloadCancel := context.WithTimeout(ctx, configLoadTimeout)
	var loadErr error
	ss, loadErr = s.cfgStore.LoadServerSettings(reloadCtx)
	reloadCancel()
	if loadErr != nil {
		s.logger.Error("maxmind update: reload settings failed", "error", loadErr)
		return
	}
	s.registerMMDBLookups(ss.Lookup, registry)

	// Update the last-update timestamp.
	saveCtx, saveCancel := context.WithTimeout(ctx, configLoadTimeout)
	defer saveCancel()
	ss.MaxMind.LastUpdate = time.Now()
	if err := s.cfgStore.SaveServerSettings(saveCtx, ss); err != nil {
		s.logger.Warn("maxmind update: save timestamp failed", "error", err)
	}
}
