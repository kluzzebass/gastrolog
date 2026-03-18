#!/bin/sh
# PreToolUse hook for Bash: warn before running dcat close.
# Reminds the agent to get explicit user approval before closing issues.

command=$(jq -r '.tool_input.command // ""' 2>/dev/null)

if echo "$command" | grep -q "dcat close"; then
  echo '{"systemMessage":"STOP: Do NOT close issues without explicit user approval. Set the issue to in_review, ask the user to test, and WAIT for them to say you can close it."}'
fi
