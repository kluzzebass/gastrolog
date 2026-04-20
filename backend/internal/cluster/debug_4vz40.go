package cluster

// #region agent log
// Session-only debug logger for gastrolog-4vz40 (cluster-wide data
// destruction cascade). This file investigates WHY node3 got "concussed"
// — i.e. what causes the gRPC StreamForwardRecords stream to EOF in the
// first place. Remove this file once the bug is diagnosed and verified.

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

const debug4vz40Path = "/Users/kluzz/Code/gastrolog/.cursor/debug-526fda.log"

var debug4vz40Mu sync.Mutex

func debugLog4vz40(location, message string, data map[string]any) {
	debug4vz40Mu.Lock()
	defer debug4vz40Mu.Unlock()
	f, err := os.OpenFile(debug4vz40Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	payload := map[string]any{
		"sessionId":    "526fda",
		"hypothesisId": "4vz40-concussion",
		"location":     location,
		"message":      message,
		"data":         data,
		"timestamp":    time.Now().UnixMilli(),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_, _ = f.Write(b)
	_, _ = f.Write([]byte("\n"))
}

// #endregion
