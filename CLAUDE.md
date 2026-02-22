# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Do not suggest creating PRs.

## Always create new branches before picking up issues.

## This project uses react compiler, so no there need to memoize everything.

## Renaming: Always Rename Through the Entire Stack

When renaming a concept, type, field, or variable, always rename it consistently across the **entire stack**: proto definitions, generated code (re-run `buf generate`), Go backend types, SQLite schema/migrations, frontend TypeScript types, UI labels, and tests. Never leave a partial rename where different layers use different names for the same concept.

## Data Integrity: Facts Before Speculation

Never present derived, heuristic, or approximate data as if it were authoritative. If information comes directly from the system (stored in a record, returned by an API), show it. If it's reconstructed client-side via heuristics (regex extraction, approximation of server logic), either don't show it or label it clearly as derived. When in doubt, leave it out — showing wrong data is worse than showing less data.

## Project Overview

GastroLog is a log management system built around chunk-based storage and indexing.

- **Backend** (`backend/`): Go 1.25+, Connect RPC server, chunk/index engines
- **Frontend** (`frontend/`): React 19 + Vite 7 + TypeScript + Tailwind v4 + Bun
- **Proto** (`backend/api/proto/`): Shared protobuf definitions, generated for both Go and TypeScript

Module name: `gastrolog` (local module, not intended for external import)

## Proto / API Contract

Proto definitions live in `backend/api/proto/gastrolog/v1/`. Changes require regenerating both sides:

```bash
cd backend/api/proto && buf generate   # Go
cd frontend && buf generate            # TypeScript
```

Services: QueryService (Search, Follow, Explain), StoreService, ConfigService, LifecycleService.

## Repository Structure

```
backend/          Go backend (see backend/CLAUDE.md)
frontend/         React frontend (see frontend/CLAUDE.md)
```

See the CLAUDE.md in each directory for stack-specific guidance.

## Third-Party Assets

- **Stomach icon** (`frontend/public/favicon.svg`): By [Delapouite](https://delapouite.com/), from [Game-icons.net](https://game-icons.net/1x1/delapouite/stomach.html), licensed under [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/).


## Issue tracking

This project uses **dcat** for issue tracking and **git** for version control. You MUST run `dcat prime` for instructions.
Then run `dcat list --agent-only` to see the list of issues. Generally we work on bugs first, and always on high priority issues first.

ALWAYS run `dcat update --status in_progress $issueId` when you start working on an issue.

When picking up a child issue, consider whether it can truly be started before the parent is done. Parent-child is organizational, not blocking. If the child genuinely needs the parent to complete first, add an explicit dependency with `dcat dep <child_id> add --depends-on <parent_id>`.

It is okay to work on multiple issues at the same time - just mark all of them as in_progress, and ask the user which one to prioritize if there is a conflict.

If the user brings up a new bug, feature or anything else that warrants changes to the code, first ask if we should create an issue for it before you start working on the code.

When creating a **question** issue (type: question), always draft the title and description first and confirm them with the user before running `dcat create`. Questions capture decisions and context, so the wording matters.

### Issue Status Workflow

Status progression: `open` → `in_progress` → `in_review` → `closed`

When starting work:

```bash
dcat show $issueId                         # Read full issue details first
dcat update --status in_progress $issueId  # Then mark as in progress
```

Always create issue branches, and include the issue ID in the branch name.

When work is complete and ready for user review:

```bash
dcat update --status in_review $issueId
```

If changes are needed after review, set back to in_progress:

```bash
dcat update --status in_progress $issueId
```

### Closing Issues - IMPORTANT

NEVER close issues without explicit user approval. When work is complete:

1. Set status to `in_review`: `dcat update --status in_review $issueId`
2. Ask the user to test
3. Ask if we can close it: "Can I close issue [id] '[title]'?"
4. Only run `dcat close` after user confirms.
5. Upon closing, check in the issue list, commit, merge and push the changes.
