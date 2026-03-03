# Nodes

The Nodes tab in [Settings](settings:nodes) manages cluster membership and node identity. Each node in the cluster appears as a card showing its name, role, suffrage, and online status.

## Node Names

Every node has a display name used throughout the UI (inspector, badges, logs). Names are auto-generated on first start but can be changed at any time. Names are cosmetic — they don't affect cluster operation.

## Viewing Cluster Info

When running in cluster mode, the Nodes tab shows a **cluster info card** with:

- **Cluster Address** — the address other nodes use to join (e.g., `10.0.0.1:4566`)
- **Join Token** — the enrollment token new nodes need to join
- **Join Command** — a ready-to-copy CLI command for starting a new node

The join token is masked by default. Click the eye icon to reveal it, or use the copy buttons to copy it to your clipboard.

## Joining a Cluster

A single-node server can join an existing cluster from this tab. Enter the leader's cluster address and join token, then confirm. This operation:

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

Removing a node evicts it from the cluster entirely. The removed node stops receiving replicated state and must re-join with a fresh token if needed. You cannot remove the local node — only remote nodes can be evicted.

## Offline Nodes

Nodes that haven't broadcast stats recently show an **offline** badge. This typically means the node is down or unreachable. Offline nodes still count toward quorum if they're voters — if too many voters go offline, the cluster loses write availability until quorum is restored.
