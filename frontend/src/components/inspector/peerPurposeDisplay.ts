/** Canonical cluster-port purpose order (matches backend cluster.AllPurposes). */
export const PEER_PURPOSE_SLOTS = [
  { id: "search", emoji: "🔍", label: "search" },
  { id: "chunk-apply", emoji: "📦", label: "chunk apply" },
  { id: "vault-apply", emoji: "🏛", label: "vault apply" },
  { id: "broadcast", emoji: "📡", label: "broadcast" },
  { id: "forward", emoji: "➡️", label: "forward" },
  { id: "fwd-rpc", emoji: "🔀", label: "forward RPC" },
  { id: "eviction", emoji: "🚪", label: "eviction" },
  { id: "remove-node", emoji: "➖", label: "remove node" },
  { id: "suffrage", emoji: "🗳️", label: "suffrage" },
  { id: "chunk-xfer", emoji: "📤", label: "chunk transfer" },
  { id: "chunk-wait", emoji: "⏳", label: "chunk wait" },
  { id: "replicate", emoji: "🔁", label: "replicate" },
  { id: "repl-catchup", emoji: "🔄", label: "replicate catchup" },
  { id: "segment-pull", emoji: "📥", label: "segment pull" },
  { id: "file-xfer", emoji: "📁", label: "file transfer" },
  { id: "raft", emoji: "⚓", label: "raft" },
] as const;

export type PeerPurposeId = (typeof PEER_PURPOSE_SLOTS)[number]["id"];

const slotById = new Map<string, (typeof PEER_PURPOSE_SLOTS)[number]>(
  PEER_PURPOSE_SLOTS.map((slot) => [slot.id, slot]),
);

export function peerPurposeSlot(purposeId: string) {
  return slotById.get(purposeId);
}
