# Backend CLAUDE.md

See the root `CLAUDE.md` for Go style rules, architecture, design patterns, and directory layout.

This file covers practical commands for working in the backend directory.

## Build & Test

All commands run from `backend/`.

```bash
go build ./...                                    # Build all packages
go test ./...                                     # Run all tests
go test -v ./internal/chunk/file/                 # Single package
go test -v -run TestRecordRoundTrip ./internal/chunk/file/  # Single test
go test -race ./internal/index/...                # Race detector
go mod tidy                                       # Clean dependencies
```

## Running

```bash
just run                    # Server with config.json
just repl                   # Server + interactive REPL
just pprof                  # Server + pprof on :6060
```

The server listens on `:8080` by default (Connect RPC / gRPC-Web).

CLI flags: `-server`, `-repl`, `-config <path>`, `-pprof <addr>`.

## Proto Generation

Proto definitions live in `api/proto/gastrolog/v1/`. Generated Go code goes to `api/gen/gastrolog/v1/`.

```bash
cd api/proto && buf generate      # Regenerate Go code
cd ../../frontend && buf generate # Regenerate TypeScript code (from frontend dir)
```

Always regenerate both backend and frontend after proto changes.

## Data Directory

File-based stores write to `data/` by default (configured in `config.json`). Chunk directories are named by their 13-char base32hex ChunkID. Delete `data/` to start fresh after format changes.
