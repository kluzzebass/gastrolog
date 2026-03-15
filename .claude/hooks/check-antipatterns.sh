#!/bin/bash
# PostToolUse hook: check edited Go files for known anti-patterns.
set -e

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

# Only check Go files.
[[ "$FILE_PATH" == *.go ]] || exit 0

# Skip test files — they may legitimately use these patterns.
[[ "$FILE_PATH" != *_test.go ]] || exit 0

WARNINGS=""

if grep -n 'os\.ReadFile' "$FILE_PATH" >/dev/null 2>&1; then
  WARNINGS+="os.ReadFile detected — should this be mmap or streaming?\n"
fi

if grep -n 'io\.ReadAll' "$FILE_PATH" >/dev/null 2>&1; then
  WARNINGS+="io.ReadAll detected — is the size bounded? Should this be streaming?\n"
fi

if [ -n "$WARNINGS" ]; then
  REASON=$(printf "Anti-pattern warning in %s:\n%b\nCheck engineering principles: RAM is a limited resource. If it can stay in a file, it stays in a file." "$FILE_PATH" "$WARNINGS")
  jq -n --arg reason "$REASON" '{decision: "block", reason: $reason}'
  exit 0
fi

exit 0
