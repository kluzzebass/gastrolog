# Jobs

The Jobs tab shows background work that GastroLog performs automatically.

## Tasks

One-time operations like chunk [rotation](help:policy-rotation), [index builds](help:indexers), [retention](help:policy-retention) sweeps, and reindexing. Each task shows its description, status (pending, running, completed, or failed), and progress. Failed tasks include error details.

## Scheduled Jobs

Recurring operations that run on a timer or cron schedule. Each shows its name, schedule, time since last run, and countdown to the next run.

The **Max Concurrent Jobs** setting in [Service settings](help:service-settings) controls how many tasks can run in parallel.
