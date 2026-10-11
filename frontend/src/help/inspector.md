# Inspector

The Inspector shows the runtime state of your GastroLog instance — what [vaults](help:inspector-vaults) exist, how much data they hold, the disk-guard state of each [storage](help:inspector-storages) they're placed on, which [ingesters](help:inspector-ingesters) are running, what [background jobs](help:inspector-jobs) are in progress, and [system metrics](help:inspector-system) for each node. Open it from the header bar.

## Two-mode layout

In a [cluster](help:clustering), the Inspector offers two ways to view data:

- **Entities mode** (default): the left pane lists entity types (Vaults, Storages, Ingesters, Routes, Jobs, System), each with a count — hover it for what is counted: vaults configured, file storages, ingesters configured, routes configured, jobs active or scheduled, live nodes.
- **Nodes mode**: the left pane lists cluster nodes with their [Raft role](help:clustering) (leader, follower) and [suffrage](help:clustering) (voter, nonvoter), a [node-state badge](help:clustering-nodes#node-states) when the node is not live, and **offline** when the cluster has stopped hearing from it. The right pane shows all entity sections for the selected node.

Use the toggle at the top of the left pane to switch between modes. In single-node deployments the toggle is hidden and entities mode is used automatically.

## Cluster Status

In Nodes mode, each node in the left pane shows its current role and online status. The leader node is marked with a badge. Nodes that haven't [broadcast](help:clustering-broadcasting) recently appear as offline. This gives a quick at-a-glance view of cluster health without opening Settings.

Everything here is read-only. To change configuration, use [Settings](help:settings) instead.

## Partial results

Vault chunk lists and index lists are merged from every node hosting the vault. When a hosting node did not answer, the merge still renders and carries a **partial — N nodes unreached** badge: the figures leave out whatever those nodes hold. Hover the badge for each unreached node and why it dropped out. A node that answered "I hold nothing" is not unreached; only a node that failed to answer counts.
