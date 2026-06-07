#!/bin/bash
# Block agents from merging PRs via gh CLI. User merges on GitHub.
# Push-to-main is already blocked by GitHub branch protection.

set -euo pipefail

input=$(cat)
command=$(echo "$input" | jq -r '.command // empty')

if echo "$command" | grep -qE '(^|[;&|]|&&[[:space:]]*)gh pr merge'; then
  jq -n \
    --arg um "Blocked: gh pr merge. Merge PRs into main (or any protected branch) on GitHub, not via the CLI." \
    --arg am "Do not run gh pr merge. The user completes PR merges in the GitHub UI." \
    '{permission: "deny", user_message: $um, agent_message: $am}'
  exit 2
fi

echo '{ "permission": "allow" }'
exit 0
