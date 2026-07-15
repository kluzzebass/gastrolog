# MEMORY.md — agent reminders

# DO
# NOT
# FUCKING
# TOUCH
# THE
# CLUSTER!

**Unless the user explicitly tells you to in that conversation — not implied by a plan, not “validation,” not “soak test on live.”**

## Forbidden without explicit user instruction

- `just cluster-kill`, `just cluster-run`, `scripts/cluster.sh kill/run/init`
- `pkill` / signals against `gastrolog` server processes
- Starting, stopping, or restarting cluster nodes (including background `go run … server`)
- “Recovering” or “validating” by bouncing the user’s running cluster
- Probing live nodes for acceptance (`curl …/readyz`, head/ counts on `/Volumes/Storage/Gastrolog`, etc.) when the user did not ask

## Allowed

- In-process / `go test` harnesses (orchestrator reliability tests, etc.)
- Code changes and unit tests
- Telling the user what **they** can run when they are ready

The user’s dev cluster (often `/Volumes/Storage/Gastrolog` or `data/node*`) is **theirs**. Killing it destroys hours of soak state and trust.
