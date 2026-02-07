package file

import (
	"encoding/json"
	"fmt"
	"os"
)

// migration transforms a JSON config from one version to the next.
type migration struct {
	from    int
	to      int
	migrate func(raw json.RawMessage) (json.RawMessage, error)
}

// migrations is the ordered list of JSON config migrations.
// Empty for now — version 1 is the initial format.
var migrations []migration

// migrateFile runs all necessary migrations on the config file.
// Before each migration step, the current file is backed up.
func migrateFile(path string, data []byte, fromVersion int) error {
	current := fromVersion

	for _, m := range migrations {
		if m.from != current {
			continue
		}

		// Back up current file before migrating.
		backupPath := fmt.Sprintf("%s.v%d.bak", path, current)
		if err := os.WriteFile(backupPath, data, 0644); err != nil {
			return fmt.Errorf("backup before migration v%d→v%d: %w", m.from, m.to, err)
		}

		migrated, err := m.migrate(json.RawMessage(data))
		if err != nil {
			return fmt.Errorf("migration v%d→v%d: %w", m.from, m.to, err)
		}

		// Write migrated data atomically.
		tmpPath := path + ".tmp"
		if err := os.WriteFile(tmpPath, migrated, 0644); err != nil {
			return fmt.Errorf("write migrated config: %w", err)
		}
		if err := os.Rename(tmpPath, path); err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("rename migrated config: %w", err)
		}

		data = migrated
		current = m.to
	}

	if current != currentVersion {
		return fmt.Errorf("no migration path from version %d to %d", fromVersion, currentVersion)
	}

	return nil
}
