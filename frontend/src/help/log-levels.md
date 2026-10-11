# Log Levels

The Log Levels tab sets how much each part of GastroLog logs. Every node applies the same rules: they are stored in the cluster configuration and reach every node through cluster-ctl Raft, so a change made on one node is in effect everywhere.

## Rules

A rule is a **pattern** and a **level**. The default level applies to every component no rule matches.

Patterns are dot-separated component paths. `*` matches exactly one segment, `**` matches any depth:

| Pattern | Matches |
|---------|---------|
| `cluster` | the `cluster` component only |
| `chunk-manager.*` | `chunk-manager.file`, `chunk-manager.memory` — not `chunk-manager` itself |
| `orchestrator.**` | `orchestrator` and everything beneath it |

When several rules match one component, the most specific pattern wins. Specificity is scored per segment — a literal segment counts most, `*` less, `**` least — and the highest total wins; a longer pattern wins a tie. Two rules with the same pattern are refused.

Rules are saved together with Save; Discard drops unsaved edits.

## Components

The Components card lists every component path the running binary registers, with the level each one is logging at right now and where that level came from:

| Source | Meaning |
|--------|---------|
| `default` | no rule matches; the default level applies |
| `exact` | a rule names this exact path |
| `glob` | a `*` or `**` rule matches this path |

Hovering a component highlights the rule that produced its level, and hovering a rule highlights the components it covers. The description beside each path says what the component does, so a path can be chosen for a rule without knowing the code.
