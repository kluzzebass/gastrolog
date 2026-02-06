# GastroLog Frontend Roadmap

## Current State

React 19 + Vite 7 + TypeScript + Tailwind v4 + Bun. Connect RPC client talks to Go backend. Observatory design theme with dark/light mode. Single-page app with search, explain, histogram, infinite scroll, and detail panel.

### What's Done

- **API Integration**: buf-generated TypeScript types, Connect RPC client, Vite proxy to backend
- **Hooks**: `useSearch` (streaming + infinite scroll + resume tokens), `useExplain`, `useStores`, `useHistogram`
- **Search**: Token and boolean expression queries, `key=value` filters, time range, reverse order
- **Results**: Streaming results, token/KV highlighting, virtual scroll, keyboard nav (j/k)
- **Detail Panel**: Timestamps with relative time, message byte size, extracted KV pairs, attributes, chunk reference
- **Explain**: Full query plan visualization with index pipeline per chunk
- **Histogram**: Time distribution of results
- **Theme**: Dark/light mode toggle, Observatory design with copper/amber accents
- **ChunkID**: 13-char base32hex timestamp strings (no more UUID byte decoding)
- **SourceTS**: Displayed in detail panel when available

## Phase 1: Live Tail (Follow)

### 1.1 Streaming
- [ ] Wire up `QueryService.Follow` streaming RPC
- [ ] Auto-scroll when at bottom, pause when scrolled up
- [ ] Show "X new logs" indicator when paused
- [ ] Rate limiting for high-volume streams

### 1.2 Follow UI
- [ ] Follow/Pause toggle button
- [ ] Connection status indicator
- [ ] Reconnection on disconnect
- [ ] Filter-while-following

## Phase 2: Store & Chunk Management

### 2.1 Store List
- [ ] Wire up `StoreService.ListStores` RPC (hook exists, needs UI)
- [ ] Display store metadata (record count, byte size, chunk count)
- [ ] Store filtering/selection for queries
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
- [ ] Shareable query URLs
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
