# CLAUDE.md

The role of this file is to describe common mistakes and confusion points that agents might encounter as they work in this project. If you ever encounter something in the project that surprises you, please alert the developer working with you and indicate that this is the case in the CLAUDE.md file to help prevent future agents from having the same issue.

## Do not suggest creating PRs.

## Always create new branches before picking up issues.

## React compiler is enabled — never use React.memo, useMemo, or useCallback.

## Always use Bun — `bun test`, `bunx tsc`, `bun install`. Never npm/npx.

## Renaming: Always Rename Through the Entire Stack

When renaming a concept, type, field, or variable, rename it consistently across the entire stack: proto definitions, generated code (re-run `buf generate`), Go backend types, frontend TypeScript types, UI labels, and tests. Never leave a partial rename.

## Proto changes require regenerating both sides

```bash
just gen                    # both sides at once
just backend gen            # Go only
just frontend gen           # TypeScript only
```

## Data Integrity: Facts Before Speculation

Never present derived or approximate data as if it were authoritative. If it comes from the system, show it. If it's reconstructed client-side via heuristics, either don't show it or label it as derived. When in doubt, leave it out.

## Issue tracking

This project uses **dcat** for issue tracking. Run `dcat prime` for instructions, then `dcat list --agent-only` for the issue list. Work on bugs first, high priority first.

ALWAYS run `dcat update --status in_progress $issueId` when starting work.

When picking up a child issue, consider whether it can truly be started before the parent is done. If the child genuinely needs the parent first, add a dependency with `dcat dep <child_id> add --depends-on <parent_id>`.

It is okay to work on multiple issues at the same time — just mark all as in_progress, and ask the user which to prioritize if there is a conflict.

If the user brings up a new bug, feature or anything else that warrants code changes, first ask if we should create an issue before starting.

When creating a **question** issue, always draft the title and description first and confirm with the user before running `dcat create`.

### Issue Status Workflow

`open` → `in_progress` → `in_review` → `closed`

Always create issue branches with the issue ID in the branch name.

### Closing Issues

NEVER close issues without explicit user approval:

1. Set status to `in_review`
2. Ask the user to test
3. Ask if we can close it
4. Only run `dcat close` after user confirms
5. Upon closing, commit, merge and push
