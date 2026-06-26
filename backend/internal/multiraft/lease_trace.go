package multiraft

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

// clusterConfigGroupID is the multiraft group for cluster-ctl Raft RPCs.
// Must stay aligned with cluster.ConfigGroupID ("config").
const clusterConfigGroupID = "config"

const (
	leaseTraceSlowConfigHeartbeat = 50 * time.Millisecond
	leaseTraceSlowOtherHeartbeat  = 100 * time.Millisecond
	leaseTraceSlowAppend          = 200 * time.Millisecond
)

var (
	leaseTraceEnabled atomic.Bool
	leaseTraceLogger  atomic.Pointer[slog.Logger]
)

func init() {
	if v := os.Getenv("GLOG_RAFT_LEASE_TRACE"); v != "" && v != "0" {
		leaseTraceEnabled.Store(true)
	}
}

// EnableLeaseTrace turns on slow-path logging for Raft lease-sensitive RPCs
// (cluster config group heartbeats first). Also enabled when
// GLOG_RAFT_LEASE_TRACE is set in the environment.
func EnableLeaseTrace(logger *slog.Logger, enabled bool) {
	if v := os.Getenv("GLOG_RAFT_LEASE_TRACE"); v != "" && v != "0" {
		enabled = true
	}
	leaseTraceEnabled.Store(enabled)
	if logger != nil {
		leaseTraceLogger.Store(logger)
		if enabled {
			logger.Info("raft lease heartbeat tracing enabled")
		}
	}
}

func leaseTraceLog() *slog.Logger {
	if l := leaseTraceLogger.Load(); l != nil {
		return l.With("component", "raft.lease_trace")
	}
	return slog.Default().With("component", "raft.lease_trace")
}

func groupIDString[K comparable](id K) string {
	return fmt.Sprintf("%v", id)
}

func isClusterConfigGroup[K comparable](id K) bool {
	return groupIDString(id) == clusterConfigGroupID
}

func traceOutboundAppendEntries[K comparable](
	groupID K,
	target raft.ServerAddress,
	args *raft.AppendEntriesRequest,
	connWait, rpcDur, total time.Duration,
	err error,
) {
	if !leaseTraceEnabled.Load() {
		return
	}
	hb := isHeartbeat(args)
	configGroup := isClusterConfigGroup(groupID)
	slow := err != nil
	if hb {
		if configGroup && total >= leaseTraceSlowConfigHeartbeat {
			slow = true
		} else if !configGroup && total >= leaseTraceSlowOtherHeartbeat {
			slow = true
		}
	} else if total >= leaseTraceSlowAppend {
		slow = true
	}
	if !slow {
		return
	}

	level := slog.LevelWarn
	if err == nil && total < 200*time.Millisecond {
		level = slog.LevelInfo
	}

	leaseTraceLog().Log(context.Background(), level, "outbound AppendEntries",
		"group", groupIDString(groupID),
		"peer", string(target),
		"heartbeat", hb,
		"conn_wait_ms", connWait.Milliseconds(),
		"rpc_ms", rpcDur.Milliseconds(),
		"total_ms", total.Milliseconds(),
		"term", args.Term,
		"err", err,
	)
}

func traceInboundAppendEntries[K comparable](
	groupID K,
	command any,
	fastPath bool,
	queueWait, waitDur, total time.Duration,
	err error,
) {
	if !leaseTraceEnabled.Load() {
		return
	}
	req, ok := command.(*raft.AppendEntriesRequest)
	if !ok {
		return
	}
	hb := isHeartbeat(req)
	configGroup := isClusterConfigGroup(groupID)
	slow := err != nil
	if hb {
		if configGroup && total >= leaseTraceSlowConfigHeartbeat {
			slow = true
		} else if !configGroup && total >= leaseTraceSlowOtherHeartbeat {
			slow = true
		}
	} else if total >= leaseTraceSlowAppend {
		slow = true
	}
	if !slow {
		return
	}

	level := slog.LevelWarn
	if err == nil && total < 200*time.Millisecond {
		level = slog.LevelInfo
	}

	args := []any{
		"group", groupIDString(groupID),
		"heartbeat", hb,
		"fast_path", fastPath,
		"queue_wait_ms", queueWait.Milliseconds(),
		"handler_wait_ms", waitDur.Milliseconds(),
		"total_ms", total.Milliseconds(),
		"term", req.Term,
		"err", err,
	}
	if !hb {
		args = append(args, "entries", len(req.Entries))
	}
	leaseTraceLog().Log(context.Background(), level, "inbound AppendEntries", args...)
}
