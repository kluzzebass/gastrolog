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
