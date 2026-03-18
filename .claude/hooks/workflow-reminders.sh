#!/bin/sh
# PreToolUse hook for Bash: context-aware reminders for common commands.
# Catches workflow mistakes before they happen by injecting system messages.
# Rules sourced from CLAUDE.md.

command=$(jq -r '.tool_input.command // ""' 2>/dev/null)

case "$command" in
  *"dcat close"*)
    echo '{"systemMessage":"STOP: NEVER close issues without explicit user approval. The workflow is: (1) set status to in_review, (2) ask the user to test, (3) ask if we can close it, (4) only run dcat close after user confirms, (5) upon closing, commit, merge and push."}'
    ;;
  *"dcat show"*|*"dcat update --status in_progress"*)
    echo '{"systemMessage":"REMINDER: Always create a new branch with the issue ID in the name BEFORE starting work (git checkout -b gastrolog-XXXXX). Do NOT work directly on main."}'
    ;;
esac
