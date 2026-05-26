package orchestrator

import (
	"gastrolog/internal/spool"
	spoolfile "gastrolog/internal/spool/file"
	spoolmem "gastrolog/internal/spool/memory"
)

func (o *Orchestrator) createVaultSpoolStore(v *Vault) *vaultSpoolStore {
	var store spool.Store = spoolmem.NewManager()
	if v.Instance != nil && v.Instance.SpoolDir != "" {
		if fm, err := spoolfile.NewManager(spoolfile.Config{Dir: v.Instance.SpoolDir}); err == nil {
			store = fm
		} else {
			o.vaultOpsLogger.Warn("spool: file backend unavailable, using memory",
				"vault", v.ID, "dir", v.Instance.SpoolDir, "error", err)
		}
	}
	ss := newVaultSpoolStore(v.ID, store)
	if v.Instance != nil && v.Instance.Query != nil {
		v.Instance.Query.SetSpoolAnchorReader(ss)
	}
	return ss
}
