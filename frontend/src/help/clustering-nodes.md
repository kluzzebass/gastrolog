# Nodes

The Nodes tab in [Settings](settings:nodes) manages cluster membership and node identity. Each node in the cluster appears as a card showing its name, role, suffrage, and online status.

## Node Names

Every node has a display name used throughout the UI (inspector, badges, logs). Names are auto-generated on first start but can be changed at any time. Names are cosmetic — they don't affect cluster operation.

## Viewing Cluster Info

When running in cluster mode, the Nodes tab shows a **cluster info card** with:

- **Cluster Address** — the address other nodes use to join (e.g., `10.0.0.1:4566`)
- **Join Token** — the enrollment token new nodes need to join
- **Join Command** — a ready-to-copy CLI command for starting a new node. It names the cluster address and the token and nothing else: the new node's own cluster listen address stays at the server default (`:4566`); add `--cluster-addr` yourself if that node must listen elsewhere.

The join token is masked by default. Click the eye icon to reveal it, or use the copy buttons to copy it to your clipboard.

The **this node** badge marks the node answering your requests. It is shown only while every response in this session has come from the same node; behind a load balancer that spreads requests across nodes, no node is marked, because none is "this" one.

## Joining a Cluster

A single-node server can join an existing cluster from this tab. Enter the leader's cluster address and join token, then confirm. The **Join Cluster** card is always shown; on a node that is already one of several members it is disabled with that reason, since only a single-node cluster can join another. This operation:

- Replaces the node's local configuration with the cluster's replicated state
- Backs up the previous Raft state to `<home>/raft.bak.<timestamp>`
- Keeps the API port running throughout (the cluster port restarts briefly)
- Is **irreversible** — the old single-node configuration is replaced

You can also join at startup via CLI flags: `--join-addr` and `--join-token`.

## Promoting and Demoting

Voter nodes participate in leader elections and quorum. Nonvoter nodes receive replicated state but act as read replicas.

- **Promote** — changes a nonvoter to a voter, adding it to the quorum
- **Demote** — changes a voter to a nonvoter, removing it from the quorum

The last remaining voter cannot be demoted — at least one voter must always exist.

## Removing Nodes

Removing a node evicts it from the cluster entirely. The removed node stops receiving replicated state and must re-join with a fresh token if needed. You cannot remove the local node — only peer nodes can be evicted.

Removal is refused when it would cost data or redundancy: if the node holds the only copy of a vault, or if removing it would leave a vault below its replication factor with no other eligible node to re-place onto. The Nodes tab works this out from the vault placements before you ask: **Remove** stays disabled and names the affected vaults, and the confirm step names the vaults that lose a copy on the node and are re-placed elsewhere. The leader checks the same rule again on the request. Lower the replication factor, drain those vaults to other nodes, or add an eligible node — or use `gastrolog cluster remove-node <node> --force` to remove anyway and accept the loss.

## Node States

Each node card carries its role (**leader** or **follower**) and suffrage (**voter** or **nonvoter**) badges, and a state badge whenever the node is not simply live:

| Badge | Meaning |
|-------|---------|
| _(none)_ | Live: a cluster member in Raft contact |
| **unreachable** | The cluster has lost Raft contact with the node — check the process and its network; its placements wait for it |
| **maintenance** | An operator declared the node absent; the cluster expects it back and keeps its placements |
| **draining** | Chunks are being transferred off the node ahead of its removal |
| **decommissioning** | The node is being removed from the cluster and stops receiving placements |
| **offline** | A live node whose stats broadcast has stopped arriving — it is down or unreachable; the badge shows how long it has been silent |

The duration after a state badge is how long the node has been in that state.

## Offline Nodes

Nodes that haven't broadcast stats recently show an **offline** badge. This typically means the node is down or unreachable. Offline nodes still count toward quorum if they're voters — if too many voters go offline, the cluster loses write availability until quorum is restored.
