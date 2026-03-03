# Inspector

The Inspector shows the runtime state of your GastroLog instance — what [vaults](help:inspector-vaults) exist, how much data they hold, which [ingesters](help:inspector-ingesters) are running, what [background jobs](help:inspector-jobs) are in progress, and [system metrics](help:inspector-system) for each node. Open it from the header bar.

## Two-mode layout

In a [cluster](help:clustering), the Inspector offers two ways to view data:

- **Entities mode** (default): the left pane lists entity types (Vaults, Ingesters, Jobs, System). Items in the right pane are grouped by node.
- **Nodes mode**: the left pane lists cluster nodes with their [Raft role](help:clustering) (leader, follower) and [suffrage](help:clustering) (voter, nonvoter). The right pane shows all entity sections for the selected node.

Use the toggle at the top of the left pane to switch between modes. In single-node deployments the toggle is hidden and entities mode is used automatically.

## Cluster Status

In Nodes mode, each node in the left pane shows its current role and online status. The leader node is marked with a badge. Nodes that haven't [broadcast](help:clustering-broadcasting) recently appear as offline. This gives a quick at-a-glance view of cluster health without opening Settings.

Everything here is read-only. To change configuration, use [Settings](help:settings) instead.
