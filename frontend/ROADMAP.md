# GastroLog Frontend Roadmap

## Current State

React 19 + Vite 7 + TypeScript + Tailwind v4 + Bun. Connect RPC client talks to Go backend. Observatory design theme with dark/light mode. Single-page app with search, explain, histogram, infinite scroll, and detail panel.

### What's Done

- **API Integration**: buf-generated TypeScript types, Connect RPC client, Vite proxy to backend
- **Hooks**: `useSearch` (streaming + infinite scroll + resume tokens), `useFollow` (streaming), `useExplain`, `useStores`, `useHistogram`
- **Search**: Token and boolean expression queries, `key=value` filters (including quoted values), time range, reverse order
- **Follow**: Live tail via `QueryService.Follow` streaming RPC, auto-scroll, follow/stop toggle
- **Routing**: `/search?q=...` and `/follow?q=...` routes — bookmarkable, shareable, browser back/forward
- **Results**: Streaming results, token/KV highlighting, virtual scroll, Escape to deselect
- **Detail Panel**: Timestamps with relative time, message byte size, extracted KV pairs, attributes, chunk reference with click-to-filter, pin to persist across queries
- **Stores**: Live store list with auto-refresh, click to filter by store (toggle on/off), total summary
- **Fields**: Extracted KV field explorer with click-to-filter (toggle), active filter highlighting, quoted value support
- **Explain**: Full query plan visualization with index pipeline per chunk
- **Histogram**: Time distribution with severity-stacked bars (error/warn/info/debug/trace), brush selection, pan (arrows + drag), tooltip with per-level breakdown
- **Time Range**: Preset buttons (5m–30d, All), calendar date picker with time inputs, custom range via brush or manual entry
- **Query Help**: Popup showing token search, boolean operators, key=value filters, time bounds, and examples
- **Theme**: Dark/light mode toggle, Observatory design with copper/amber accents, light-mode-safe highlights
- **ChunkID**: 13-char base32hex timestamp strings (no more UUID byte decoding)
- **SourceTS**: Displayed in detail panel when available

## Phase 1: Live Tail (Follow)

### 1.1 Streaming
- [x] Wire up `QueryService.Follow` streaming RPC
- [x] Auto-scroll when at bottom, pause when scrolled up
- [ ] Show "X new logs" indicator when paused
- [ ] Rate limiting for high-volume streams

### 1.2 Follow UI
- [x] Follow/Stop toggle button
- [ ] Connection status indicator
- [ ] Reconnection on disconnect
- [x] Filter-while-following
- [x] Route-based follow (`/follow?q=...`) — bookmarkable, browser back/forward works

## Phase 2: Store & Chunk Management

### 2.1 Store List
- [x] Wire up `StoreService.ListStores` RPC with auto-refresh
- [x] Display store metadata (record count, byte size, chunk count)
- [x] Store filtering/selection for queries (click to add `store=` filter, click again to remove)
- [ ] Store health indicators

### 2.2 Chunk Browser
- [ ] Wire up `StoreService.ListChunks` RPC
- [ ] Chunk timeline visualization
- [ ] Sealed vs active status
- [ ] Chunk detail view with index status

### 2.3 Statistics
- [ ] Wire up `StoreService.GetStats` RPC
- [ ] Dashboard widgets for key metrics
- [ ] Auto-refresh

## Phase 3: Query UX

### 3.1 Query Builder
- [x] Query help popup (syntax reference, operators, examples)
- [ ] Syntax highlighting in query input
- [ ] Query history (localStorage)
- [ ] Saved queries
- [ ] Auto-complete for known attribute keys

### 3.2 Log Context
- [ ] "Show Context" (records before/after match)
- [ ] "Jump to Time" feature
- [ ] Link related logs by trace ID

### 3.3 Export
- [ ] Export results as JSON/CSV
- [x] Shareable query URLs (route-based `/search?q=...` and `/follow?q=...`)
- [ ] Copy log lines to clipboard

## Phase 4: Polish

### 4.1 Responsive Design
- [ ] Optimize for tablet viewports
- [ ] Collapsible sidebar for mobile
- [ ] Touch-friendly controls

### 4.2 Accessibility
- [ ] ARIA labels on interactive elements
- [ ] Full keyboard navigability
- [ ] Screen reader testing
- [ ] Focus indicators

### 4.3 Performance
- [ ] Profile and optimize render performance
- [ ] Code splitting by route
- [ ] Optimize bundle size

## Priority Order

1. **Phase 1** - Live Tail (key differentiator, backend already supports it)
2. **Phase 2** - Store & Chunk Management (navigation and observability)
3. **Phase 3** - Query UX (quality of life)
4. **Phase 4** - Polish (production readiness)
