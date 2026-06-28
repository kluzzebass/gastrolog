package cluster

// Cluster-port connection purpose labels (pool bookkeeping + NodeStats inspector).
const (
	PurposeSearch      = "search"
	PurposeChunkApply  = "chunk-apply"
	PurposeVaultApply  = "vault-apply"
	PurposeBroadcast   = "broadcast"
	PurposeForward     = "forward"
	PurposeFwdRPC      = "fwd-rpc"
	PurposeEviction    = "eviction"
	PurposeRemoveNode  = "remove-node"
	PurposeSuffrage    = "suffrage"
	PurposeChunkXfer   = "chunk-xfer"
	PurposeChunkWait   = "chunk-wait"
	PurposeReplicate   = "replicate"
	PurposeReplCatchup = "repl-catchup"
	PurposeSegmentPull = "segment-pull"
	PurposeFileXfer    = "file-xfer"
	PurposeRaft        = "raft"
)
