# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## CRITICAL: DO NOT BUMP FILE VERSIONS OR CREATE MIGRATION CODE!

This project is NOT in production. When changing file formats, just change them. Do not increment version numbers or write backward-compatibility code. Delete old data and start fresh.

## Data Integrity: Facts Before Speculation

Never present derived, heuristic, or approximate data as if it were authoritative. If information comes directly from the system (stored in a record, returned by an API), show it. If it's reconstructed client-side via heuristics (regex extraction, approximation of server logic), either don't show it or label it clearly as derived. When in doubt, leave it out — showing wrong data is worse than showing less data.

## Project Overview

GastroLog is a log management system built around chunk-based storage and indexing.

- **Backend** (`backend/`): Go 1.25+, Connect RPC server, REPL, chunk/index engines
- **Frontend** (`frontend/`): React 19 + Vite 7 + TypeScript + Tailwind v4 + Bun
- **Proto** (`backend/api/proto/`): Shared protobuf definitions, generated for both Go and TypeScript

Module name: `gastrolog` (local module, not intended for external import)

## Proto / API Contract

Proto definitions live in `backend/api/proto/gastrolog/v1/`. Changes require regenerating both sides:

```bash
cd backend/api/proto && buf generate   # Go
cd frontend && buf generate            # TypeScript
```

Services: QueryService (Search, Follow, Explain), StoreService, ConfigService, LifecycleService.

## Repository Structure

```
backend/          Go backend (see backend/CLAUDE.md)
frontend/         React frontend (see frontend/CLAUDE.md)
```

See the CLAUDE.md in each directory for stack-specific guidance.
