/** Cluster-port purposes in the backend's canonical order, with their operator labels. */
export const PEER_PURPOSES = [
  { id: "search", label: "search" },
  { id: "chunk-apply", label: "chunk apply" },
  { id: "vault-apply", label: "vault apply" },
  { id: "broadcast", label: "broadcast" },
  { id: "forward", label: "forward" },
  { id: "fwd-rpc", label: "forward RPC" },
  { id: "eviction", label: "eviction" },
  { id: "remove-node", label: "remove node" },
  { id: "suffrage", label: "suffrage" },
  { id: "chunk-xfer", label: "chunk transfer" },
  { id: "chunk-wait", label: "chunk wait" },
  { id: "replicate", label: "replicate" },
  { id: "repl-catchup", label: "replicate catchup" },
  { id: "segment-pull", label: "segment pull" },
  { id: "file-xfer", label: "file transfer" },
  { id: "raft", label: "raft" },
] as const;

export function peerPurposeLabel(id: string): string {
  return PEER_PURPOSES.find((p) => p.id === id)?.label ?? id;
}

/** The given purpose IDs in canonical order; IDs the list does not know follow, sorted. */
export function orderPeerPurposes(ids: Iterable<string>): string[] {
  const wanted = new Set([...ids].filter((id) => id && id !== "unknown"));
  const known = PEER_PURPOSES.map((p) => p.id).filter((id) => wanted.has(id));
  const knownSet = new Set<string>(known);
  const extra = [...wanted].filter((id) => !knownSet.has(id)).sort();
  return [...known, ...extra];
}
