package server

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/home"
	"gastrolog/internal/lookup"
	"gastrolog/internal/system"
)

// loadInitialLookupConfig loads MMDB, HTTP, and JSON lookup tables from persisted config at startup.
func (s *Server) loadInitialLookupConfig(registry lookup.Registry) {
	if s.cfgStore == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), systemLoadTimeout)
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

// applyLookupConfig loads (or reloads) MMDB, HTTP, and JSON lookup tables from the given system.
// It also manages the maxmind-update cron job for automatic downloads.
func (s *Server) applyLookupConfig(cfg system.LookupConfig, mm system.MaxMindConfig, registry lookup.Registry) {
	// Register MMDB lookup tables (GeoIP City / ASN) from system.
	s.registerMMDBLookups(cfg, registry)

	// Register HTTP lookup tables from system.
	s.registerHTTPLookups(cfg, registry)

	// Register JSON file lookup tables from system.
	s.registerJSONFileLookups(cfg, registry)

	// Register YAML file lookup tables from system. Shares the FileLookup
	// engine with JSON; format differs only in the unmarshaler.
	s.registerYAMLFileLookups(cfg, registry)

	// Register CSV lookup tables from system.
	s.registerCSVLookups(cfg, registry)

	// Register static (inline) lookup tables from system.
	s.registerStaticLookups(cfg, registry)

	// Manage the maxmind-update cron job.
	s.manageMaxMindJob(mm, registry)
}

// registerMMDBLookups registers MMDB-backed lookup tables (GeoIP City / ASN) from system.
// Follows the same lifecycle pattern as registerJSONFileLookups: close+remove stale, create+load new.
func (s *Server) registerMMDBLookups(cfg system.LookupConfig, registry lookup.Registry) {
	ctx, cancel := context.WithTimeout(context.Background(), systemLoadTimeout)
	defer cancel()

	// Build keep set of names that should exist.
	keep := make(map[string]struct{}, len(cfg.MMDBLookups))
	for _, mcfg := range cfg.MMDBLookups {
		if mcfg.Name != "" {
			keep[mcfg.Name] = struct{}{}
		}
	}

	// Close and remove any MMDB lookups no longer in system.
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
func (s *Server) registerHTTPLookups(cfg system.LookupConfig, registry lookup.Registry) {
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

// registerJSONFileLookups registers JSON file-backed lookup tables.
func (s *Server) registerJSONFileLookups(cfg system.LookupConfig, registry lookup.Registry) {
	s.registerFileLookups("json", cfg.JSONFileLookups, registry, lookup.NewJSONFile)
}

// registerYAMLFileLookups registers YAML file-backed lookup tables.
func (s *Server) registerYAMLFileLookups(cfg system.LookupConfig, registry lookup.Registry) {
	s.registerFileLookups("yaml", cfg.YAMLFileLookups, registry, lookup.NewYAMLFile)
}

// registerFileLookups is the shared registration path for JSON and YAML
// file-backed lookups. Format differs only in the constructor — the Close /
// Load / WatchFile / DuplicateKeys lifecycle is identical.
func (s *Server) registerFileLookups(
	format string,
	entries []system.JSONFileLookupConfig,
	registry lookup.Registry,
	newFn func(lookup.FileLookupConfig) (*lookup.FileLookup, error),
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Build set of names that should exist for this format.
	keep := make(map[string]struct{}, len(entries))
	for _, cfg := range entries {
		if cfg.Name != "" {
			keep[cfg.Name] = struct{}{}
		}
	}

	// Close and remove any lookups of this format no longer in system.
	// JSONFile and YAMLFile share *FileLookup, so we filter by Format().
	for name, table := range registry {
		fl, ok := table.(*lookup.FileLookup)
		if !ok || fl.Format() != format {
			continue
		}
		if _, exists := keep[name]; !exists {
			fl.Close()
			delete(registry, name)
			s.logger.Info("removed file lookup table", "format", format, "name", name)
		}
	}

	for _, cfg := range entries {
		if cfg.Name == "" || cfg.FileID == "" {
			continue
		}

		// Close any existing lookup with the same name (stops its watcher).
		if existing, ok := registry[cfg.Name]; ok {
			if fl, ok := existing.(*lookup.FileLookup); ok {
				fl.Close()
			}
		}

		filePath := s.ResolveManagedFileByID(ctx, cfg.FileID)
		if filePath == "" {
			s.logger.Warn("file lookup source not found", "format", format, "name", cfg.Name, "file_id", cfg.FileID)
			continue
		}

		fl, err := newFn(lookup.FileLookupConfig{
			Name:         cfg.Name,
			Query:        cfg.Query,
			KeyColumn:    cfg.KeyColumn,
			ValueColumns: cfg.ValueColumns,
		})
		if err != nil {
			s.logger.Warn("failed to compile file lookup jq expression", "format", format, "name", cfg.Name, "error", err)
			continue
		}

		if err := fl.Load(filePath); err != nil {
			s.logger.Warn("failed to load file lookup", "format", format, "name", cfg.Name, "path", filePath, "error", err)
			continue
		}
		if err := fl.WatchFile(filePath); err != nil {
			s.logger.Warn("failed to watch file lookup", "format", format, "name", cfg.Name, "path", filePath, "error", err)
		}

		if dups := fl.DuplicateKeys(); dups > 0 {
			s.logger.Warn("file lookup has duplicate keys (first occurrence wins)", "format", format, "name", cfg.Name, "duplicates", dups)
		}
		registry[cfg.Name] = fl
		s.logger.Info("registered file lookup table", "format", format, "name", cfg.Name, "path", filePath)
	}
}

// registerCSVLookups registers CSV file-backed lookup tables from system.
func (s *Server) registerCSVLookups(cfg system.LookupConfig, registry lookup.Registry) {
	ctx, cancel := context.WithTimeout(context.Background(), systemLoadTimeout)
	defer cancel()

	keep := make(map[string]struct{}, len(cfg.CSVLookups))
	for _, ccfg := range cfg.CSVLookups {
		if ccfg.Name != "" {
			keep[ccfg.Name] = struct{}{}
		}
	}

	// Close and remove any CSV lookups no longer in system.
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

// registerStaticLookups registers inline static lookup tables from config into the registry.
func (s *Server) registerStaticLookups(cfg system.LookupConfig, registry lookup.Registry) {
	keep := make(map[string]struct{}, len(cfg.StaticLookups))
	for _, scfg := range cfg.StaticLookups {
		if scfg.Name != "" {
			keep[scfg.Name] = struct{}{}
		}
	}

	// Remove stale static lookups no longer in config.
	for name, table := range registry {
		if _, ok := table.(*lookup.Static); ok {
			if _, exists := keep[name]; !exists {
				delete(registry, name)
				s.logger.Info("removed static lookup table", "name", name)
			}
		}
	}

	for _, scfg := range cfg.StaticLookups {
		if scfg.Name == "" {
			continue
		}

		rows := make([]lookup.StaticRow, len(scfg.Rows))
		for i, r := range scfg.Rows {
			rows[i] = lookup.StaticRow{Values: r.Values}
		}

		registry[scfg.Name] = lookup.NewStatic(scfg.Name, scfg.KeyColumn, scfg.ValueColumns, rows)
		s.logger.Info("registered static lookup table", "name", scfg.Name, "rows", len(scfg.Rows))
	}
}

// manageMaxMindJob adds or removes the maxmind-update cron job based on system.
func (s *Server) manageMaxMindJob(mm system.MaxMindConfig, registry lookup.Registry) {
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
	loadCtx, loadCancel := context.WithTimeout(context.Background(), systemLoadTimeout)
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
	loadCtx, loadCancel := context.WithTimeout(context.Background(), systemLoadTimeout)
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
	reloadCtx, reloadCancel := context.WithTimeout(ctx, systemLoadTimeout)
	var loadErr error
	ss, loadErr = s.cfgStore.LoadServerSettings(reloadCtx)
	reloadCancel()
	if loadErr != nil {
		s.logger.Error("maxmind update: reload settings failed", "error", loadErr)
		return
	}
	s.registerMMDBLookups(ss.Lookup, registry)

	// Update the last-update timestamp.
	timeoutCtx, saveCancel := context.WithTimeout(ctx, systemLoadTimeout)
	defer saveCancel()
	ss.MaxMind.LastUpdate = time.Now()
	persistCtx := system.WithSaveServerSettingsNotifyKey(timeoutCtx, system.NotifyKeyMaxMindSettings)
	if err := s.cfgStore.SaveServerSettings(persistCtx, ss); err != nil {
		s.logger.Warn("maxmind update: save timestamp failed", "error", err)
	}
}
