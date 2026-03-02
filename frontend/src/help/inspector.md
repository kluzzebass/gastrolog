# Inspector

The Inspector shows the runtime state of your GastroLog instance — what [vaults](help:inspector-vaults) exist, how much data they hold, which [ingesters](help:inspector-ingesters) are running, what [background jobs](help:inspector-jobs) are in progress, and [system metrics](help:inspector-system) for each node. Open it from the header bar.

## Two-mode layout

In a cluster, the Inspector offers two ways to view data:

- **Entities mode** (default): the left pane lists entity types (Vaults, Ingesters, Jobs, System). Items in the right pane are grouped by node.
- **Nodes mode**: the left pane lists cluster nodes. The right pane shows all entity sections for the selected node.

Use the toggle at the top of the left pane to switch between modes. In single-node deployments the toggle is hidden and entities mode is used automatically.

Everything here is read-only. To change configuration, use [Settings](help:settings) instead.
