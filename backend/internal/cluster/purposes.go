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

// AllPurposes is the canonical display order for inspector purpose activity strips.
// Service-lane rows use the service entries; raft rows may also report PurposeRaft.
var AllPurposes = []string{
	PurposeSearch,
	PurposeChunkApply,
	PurposeVaultApply,
	PurposeBroadcast,
	PurposeForward,
	PurposeFwdRPC,
	PurposeEviction,
	PurposeRemoveNode,
	PurposeSuffrage,
	PurposeChunkXfer,
	PurposeChunkWait,
	PurposeReplicate,
	PurposeReplCatchup,
	PurposeSegmentPull,
	PurposeFileXfer,
	PurposeRaft,
}
