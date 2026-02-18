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

# Tag and push a release (triggers GitHub Actions build). Usage: just release patch
release bump:
    #!/usr/bin/env bash
    set -euo pipefail
    version=$(svu {{bump}})
    echo "Releasing $version"
    git tag "$version"
    git push origin "$version"
