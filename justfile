set dotenv-load

# Lists all available backend commands.
_default:
  @just --list

# Run frontend commands
frontend *args:
    @just --justfile ./frontend/justfile {{args}}

# Run backend commands
backend *args:
    @just --justfile ./backend/justfile {{args}}

# Regenerate protobuf code (Go + TypeScript)
gen:
    just backend gen
    just frontend gen

# Build binary with embedded frontend
build:
    just frontend build
    just backend embed-frontend
    just backend build

# Build release binaries for all platforms with embedded frontend
build-all:
    just frontend build
    just backend embed-frontend
    just backend build-all

# Build Docker image
docker tag="gastrolog:latest":
    docker build --build-arg VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo dev) -t {{tag}} .

# Run all tests (backend + frontend)
test:
    just backend test
    just frontend test

# Run full quality audit (backend + frontend)
audit:
    just backend audit
    just frontend audit

# Count lines of code, skipping generated/vendored/build artifacts
cloc:
    @echo "Repository: $(basename $(git rev-parse --show-toplevel))"
    @cloc --vcs=git --exclude-ext=lock,sum

# Start cloud storage emulators (MinIO, Azurite, fake-gcs-server)
cloud-storage-up:
    docker compose -f test/cloud-storage/compose.yml up -d

# Tear down cloud storage emulators
cloud-storage-down:
    docker compose -f test/cloud-storage/compose.yml down -v

# Start ingester integration test environment (all ingesters + real services)
ingester-integration-up:
    docker compose -f test/ingester-integration/compose.yml up --build -d

# Tear down ingester integration test environment
ingester-integration-down:
    docker compose -f test/ingester-integration/compose.yml down -v

# Tag and push to kick off draft release creation via GitHub Actions.
draft bump:
    #!/usr/bin/env bash
    set -euo pipefail
    version=$(svu {{bump}})
    echo "Drafting $version"
    git push origin HEAD
    git tag "$version"
    git push origin "$version"
    echo "GitHub Actions will build and create a draft release for $version."
    echo "Monitor: https://github.com/kluzzebass/gastrolog/actions"
    echo ""
    echo "Once the draft is ready:"
    echo "  just release-notes         # generate release notes"
    echo "  just publish $version      # publish after reviewing notes"
    echo ""
    echo "Tip: run 'just changelog <bump>' BEFORE drafting to update CHANGELOG.md"

# Publish a draft release after verifying release notes exist.
publish version="":
    #!/usr/bin/env bash
    set -euo pipefail
    version="{{version}}"
    if [ -z "$version" ]; then
        version=$(gh release list --json tagName,isDraft -q '[.[] | select(.isDraft)] | .[0].tagName' 2>/dev/null || true)
        if [ -z "$version" ]; then
            echo "Error: no draft release found. Run 'just draft <bump>' first."
            exit 1
        fi
    fi
    body=$(gh release view "$version" --json body -q .body 2>/dev/null || true)
    if [ -z "$body" ]; then
        echo "Error: release $version not found. Run 'just draft <bump>' first."
        exit 1
    fi
    status=$(gh release view "$version" --json isDraft -q .isDraft)
    if [ "$status" != "true" ]; then
        echo "Error: $version is not a draft (already published?)."
        exit 1
    fi
    # Strip auto-generated boilerplate to check for real content
    cleaned=$(echo "$body" | sed '/^## What/d; /^## New Contributors/d; /^\*\*Full Changelog\*\*/d; /^$/d; /^\* /d')
    if [ -z "$cleaned" ]; then
        echo "Error: release $version has no release notes (only auto-generated content)."
        echo "Edit at: https://github.com/kluzzebass/gastrolog/releases/tag/$version"
        exit 1
    fi
    echo "Publishing $version..."
    gh release edit "$version" --draft=false
    echo "Released $version"

# Generate a changelog entry for the next release using Claude Code.
changelog bump:
    #!/usr/bin/env bash
    set -euo pipefail

    version=$(svu {{bump}})

    # Find the latest release tag
    prev=$(gh release list --json tagName,isDraft -q '[.[] | select(.isDraft | not)] | .[0].tagName' 2>/dev/null || true)
    if [ -z "$prev" ]; then
        prev=$(git tag --sort=-v:refname | head -1)
    fi
    if [ -z "$prev" ]; then
        echo "Error: no previous release tag found."
        exit 1
    fi

    echo "Generating changelog for $version (since $prev)..."

    log=$(git log --format="- %s%n%b" "$prev"..HEAD | sed '/^$/d; /^Co-Authored-By:/d')
    changelog=$(cat CHANGELOG.md)

    entry=$(claude -p --output-format text --append-system-prompt "Output ONLY raw markdown. No insights, no commentary, no preamble, no sign-off." "$(cat <<PROMPT
    You are writing a CHANGELOG.md entry for GastroLog $version.

    RULES:
    - Output ONLY the changelog section. No preamble, no commentary. Just the entry.
    - Start with: ## $version — $(date +%Y-%m-%d)
    - Group changes under ### headings: Breaking Changes, Features, Performance, Fixes (omit empty groups).
    - Document what is DIFFERENT from the previous release ($prev). That is ALL the user cares about.
    - Do NOT expose internal development churn. If a feature was added then tweaked — that is ONE entry.
    - Same-cycle bug fixes (bugs introduced and fixed within this release cycle) should NOT appear.
    - Internal-only changes (CI workflows, build scripts, code cleanup, dead code removal) should NOT appear unless they affect users.
    - Use concise bullet points with **bold lead** — description format, matching the existing changelog style.
    - Keep it short and scannable. No prose paragraphs.
    - Look at the existing CHANGELOG.md for tone and formatting conventions.

    Existing CHANGELOG.md (for style reference):
    $changelog

    Git log ($prev..HEAD):
    $log
    PROMPT
    )")

    # Insert the new entry before the previous release heading
    awk -v entry="$entry" '/^## v[0-9]/ && !inserted { print entry; print ""; inserted=1 } { print }' CHANGELOG.md > CHANGELOG.md.tmp
    mv CHANGELOG.md.tmp CHANGELOG.md

    echo "Changelog updated with $version entry."
    echo "Review the changes in CHANGELOG.md before committing."

# Generate release notes for the latest draft using Claude Code. Usage: just release-notes
release-notes:
    #!/usr/bin/env bash
    set -euo pipefail

    # Find the latest draft release
    draft=$(gh release list --json tagName,isDraft -q '[.[] | select(.isDraft)] | .[0].tagName' 2>/dev/null || true)
    if [ -z "$draft" ]; then
        echo "Error: no draft release found. Run 'just draft <bump>' first."
        exit 1
    fi

    # Find the previous (non-draft) release tag
    prev=$(gh release list --json tagName,isDraft -q '[.[] | select(.isDraft | not)] | .[0].tagName' 2>/dev/null || true)
    if [ -z "$prev" ]; then
        echo "Error: no previous release found to diff against."
        exit 1
    fi

    echo "Generating release notes for $draft (since $prev)..."

    # Get the commit log and changelog
    log=$(git log --oneline "$prev".."$draft" 2>/dev/null || git log --oneline "$prev"..HEAD)
    changelog=$(cat CHANGELOG.md)

    claude -p --output-format text --append-system-prompt "Output ONLY raw markdown. No insights, no commentary, no preamble, no sign-off." "$(cat <<PROMPT
    You are writing GitHub release notes for GastroLog $draft.

    RULES:
    - Output ONLY the release notes markdown. No preamble, no commentary, no insights, no explanations, no sign-off. Just the notes.
    - Document what is DIFFERENT from the previous release ($prev). That is ALL the user cares about.
    - Do NOT expose internal development churn. If a feature was added, then changed, then had a bug fixed — that is ONE new feature, not three entries.
    - Same-cycle bug fixes (bugs introduced and fixed within this release cycle) should NOT appear. Only mention fixes for bugs that existed in $prev.
    - Use concise bullet points grouped by category (e.g. Features, Performance, Fixes, Breaking Changes).
    - Do NOT include a header/title — GitHub adds that automatically.
    - Keep it short and scannable. No prose paragraphs.
    - Use the CHANGELOG.md as the primary source of truth for what changed. The git log is supplementary context for understanding the development arc.

    CHANGELOG.md:
    $changelog

    Git log ($prev..$draft):
    $log
    PROMPT
    )" | gh release edit "$draft" --notes-file -

    echo "Release notes updated for $draft"
    echo "Review at: https://github.com/kluzzebass/gastrolog/releases/tag/$draft"
