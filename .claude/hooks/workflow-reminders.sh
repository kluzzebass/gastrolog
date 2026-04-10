#!/bin/sh
# PreToolUse hook for Bash: context-aware workflow reminders.
#
# Gate strategy: the global allow-list no longer includes dcat:*, git merge:*,
# or git push:* — those now require the permission prompt (the real gate).
# This hook adds advisory reminders for workflow discipline.
# Rules sourced from CLAUDE.md.

command=$(jq -r '.tool_input.command // ""' 2>/dev/null)

# Only match commands that START with the dangerous verb (not substrings
# inside echo/grep/test commands).
case "$command" in
  "dcat close"*)
    echo '{"systemMessage":"STOP: dcat close requires explicit user approval. Workflow: (1) set status to in_review, (2) ask the user to test, (3) wait for user to say close, (4) only then run dcat close."}'
    ;;
  "dcat show"*|"dcat update --status in_progress"*)
    echo '{"systemMessage":"REMINDER: Always create a new branch with the issue ID in the name BEFORE starting work (git checkout -b gastrolog-XXXXX). Do NOT work directly on main."}'
    ;;
esac
