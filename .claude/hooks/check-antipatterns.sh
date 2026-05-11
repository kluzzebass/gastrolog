#!/bin/bash
# PostToolUse hook: check edited Go files for known anti-patterns.
#
# Per-call suppression
# --------------------
# Some call sites pass review — e.g. small bounded TLS material loaded once
# at startup. Append a per-rule marker to the offending line to silence the
# hook for that line only:
#
#   data, err := os.ReadFile(caFile) //ok:os-readfile bounded PEM at startup
#   body, err := io.ReadAll(req.Body) //ok:io-readall capped by MaxBytesReader above
#
# Each rule has its own marker (`//ok:os-readfile`, `//ok:io-readall`) so a
# new check added later won't be silently skipped by an unrelated allowlist.
# Include a short reason after the marker so the next reader doesn't repeat
# the analysis.
set -e

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

# Only check Go files.
[[ "$FILE_PATH" == *.go ]] || exit 0

# Skip test files — they may legitimately use these patterns.
[[ "$FILE_PATH" != *_test.go ]] || exit 0

WARNINGS=""

# A rule fires when grep finds the antipattern on a line that lacks the
# per-rule marker. `grep -v` filters allowlisted lines out of the match
# stream; the outer test passes only if at least one un-suppressed match
# remains.
if grep -n 'os\.ReadFile' "$FILE_PATH" 2>/dev/null | grep -v '//ok:os-readfile' >/dev/null 2>&1; then
  WARNINGS+="os.ReadFile detected — should this be mmap or streaming? If the call is bounded and reviewed, append \`//ok:os-readfile <reason>\` to that line.\n"
fi

if grep -n 'io\.ReadAll' "$FILE_PATH" 2>/dev/null | grep -v '//ok:io-readall' >/dev/null 2>&1; then
  WARNINGS+="io.ReadAll detected — is the size bounded? Should this be streaming? If the call is bounded and reviewed, append \`//ok:io-readall <reason>\` to that line.\n"
fi

if [ -n "$WARNINGS" ]; then
  REASON=$(printf "Anti-pattern warning in %s:\n%b\nCheck engineering principles: RAM is a limited resource. If it can stay in a file, it stays in a file." "$FILE_PATH" "$WARNINGS")
  jq -n --arg reason "$REASON" '{decision: "block", reason: $reason}'
  exit 0
fi

exit 0
