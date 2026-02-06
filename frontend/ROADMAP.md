# GastroLog Frontend Roadmap

## Current State

The frontend is a React 19 + Vite application with the Editorial design theme. It currently uses mock data to demonstrate the UI. The backend exposes a Connect RPC API (gRPC-Web compatible) with Query, Store, Config, and Lifecycle services.

## Phase 1: Core Infrastructure

### 1.1 API Integration
- [ ] Generate TypeScript types from protobuf definitions using `buf`
- [ ] Set up Connect RPC client (`@connectrpc/connect-web`)
- [ ] Create API client singleton with configurable base URL
- [ ] Add connection state management (connected/disconnected/error)
- [ ] Implement retry logic with exponential backoff

### 1.2 State Management
- [ ] Set up TanStack Query for server state
- [ ] Define query keys and cache invalidation strategy
- [ ] Create custom hooks: `useSearch`, `useStores`, `useChunks`, `useStats`
- [ ] Add optimistic updates where applicable
- [ ] Implement query persistence for page refreshes

### 1.3 Routing
- [ ] Set up TanStack Router
- [ ] Define routes: `/`, `/stores`, `/stores/:id`, `/chunks/:id`, `/settings`
- [ ] Add URL-based query state (search params for filters, time range)
- [ ] Implement breadcrumb navigation

## Phase 2: Search & Results

### 2.1 Query Builder
- [ ] Parse query syntax: tokens, `key=value`, `start=`, `end=`, `limit=`
- [ ] Add syntax highlighting in query input
- [ ] Implement query history (localStorage)
- [ ] Add saved queries feature
- [ ] Auto-complete for known attribute keys

### 2.2 Search Execution
- [ ] Wire up `QueryService.Search` streaming RPC
- [ ] Handle streaming responses with progress indication
- [ ] Implement pagination with resume tokens
- [ ] Add "Load More" functionality
- [ ] Cancel in-flight queries when new search starts

### 2.3 Results Display
- [ ] Render streaming results as they arrive
- [ ] Highlight matching tokens in log messages
- [ ] Add keyboard navigation (j/k for up/down, Enter to expand)
- [ ] Implement virtual scrolling for large result sets
- [ ] Add column resizing and reordering

### 2.4 Query Plan / Explain
- [ ] Wire up `QueryService.Explain` RPC
- [ ] Visualize chunk scan strategy
- [ ] Show index usage per chunk
- [ ] Display estimated vs actual record counts

## Phase 3: Live Tail (Follow)

### 3.1 Streaming
- [ ] Wire up `QueryService.Follow` streaming RPC
- [ ] Auto-scroll when at bottom, pause when scrolled up
- [ ] Show "X new logs" indicator when paused
- [ ] Implement rate limiting for high-volume streams

### 3.2 Follow UI
- [ ] Add Follow/Pause toggle button
- [ ] Show connection status indicator
- [ ] Implement reconnection on disconnect
- [ ] Add filter-while-following capability

## Phase 4: Store & Chunk Management

### 4.1 Store List
- [ ] Wire up `StoreService.ListStores` RPC
- [ ] Display store metadata (record count, byte size, chunk count)
- [ ] Add store filtering/selection for queries
- [ ] Show store health indicators

### 4.2 Chunk Browser
- [ ] Wire up `StoreService.ListChunks` RPC
- [ ] Display chunk timeline visualization
- [ ] Show sealed vs active status
- [ ] Add chunk detail view with index status

### 4.3 Statistics
- [ ] Wire up `StoreService.GetStats` RPC
- [ ] Create dashboard widgets for key metrics
- [ ] Add sparklines for trends
- [ ] Implement auto-refresh

## Phase 5: Polish & UX

### 5.1 Theme System
- [ ] Extract CSS variables to theme config
- [ ] Implement proper dark mode toggle with persistence
- [ ] Add system preference detection
- [ ] Ensure all components respect theme

### 5.2 Responsive Design
- [ ] Optimize for tablet viewports
- [ ] Add collapsible sidebar for mobile
- [ ] Implement touch-friendly controls
- [ ] Test on various screen sizes

### 5.3 Accessibility
- [ ] Add ARIA labels to interactive elements
- [ ] Ensure keyboard navigability throughout
- [ ] Test with screen readers
- [ ] Add focus indicators
- [ ] Implement skip links

### 5.4 Performance
- [ ] Profile and optimize render performance
- [ ] Implement code splitting by route
- [ ] Add service worker for offline capability
- [ ] Optimize bundle size

## Phase 6: Advanced Features

### 6.1 Log Context
- [ ] Implement "Show Context" (records before/after match)
- [ ] Add "Jump to Time" feature
- [ ] Link related logs by trace ID

### 6.2 Export & Share
- [ ] Export results as JSON/CSV
- [ ] Generate shareable query URLs
- [ ] Copy log lines to clipboard

### 6.3 Alerts (Future)
- [ ] Define alert rules UI
- [ ] Show active alerts
- [ ] Alert history view

## Technical Debt & Cleanup

- [ ] Remove inline styles, move to CSS (fix CSS variable scoping issue)
- [ ] Add unit tests for components
- [ ] Add integration tests for API calls
- [ ] Set up Storybook for component documentation
- [ ] Add error boundaries
- [ ] Implement proper loading states

## Dependencies to Add

```bash
# API/Protobuf
bun add @bufbuild/protobuf @connectrpc/connect @connectrpc/connect-web

# Already installed but verify versions
bun add @tanstack/react-query @tanstack/react-router

# Virtual scrolling
bun add @tanstack/react-virtual

# Date handling (already have date-fns)

# Dev dependencies
bun add -d @bufbuild/buf @bufbuild/protoc-gen-es @connectrpc/protoc-gen-connect-es
```

## File Structure (Proposed)

```
frontend/src/
├── api/
│   ├── client.ts           # Connect RPC client setup
│   ├── hooks/              # TanStack Query hooks
│   │   ├── useSearch.ts
│   │   ├── useFollow.ts
│   │   ├── useStores.ts
│   │   └── useStats.ts
│   └── gen/                # Generated protobuf types
├── components/
│   ├── common/             # Shared components
│   │   ├── Button.tsx
│   │   ├── Input.tsx
│   │   └── ...
│   ├── query/              # Query-related components
│   │   ├── QueryInput.tsx
│   │   ├── QueryPlan.tsx
│   │   └── ResultsTable.tsx
│   ├── log/                # Log display components
│   │   ├── LogEntry.tsx
│   │   ├── LogDetail.tsx
│   │   └── LogList.tsx
│   └── layout/             # Layout components
│       ├── Header.tsx
│       ├── Sidebar.tsx
│       └── MainContent.tsx
├── pages/
│   ├── QueryPage.tsx
│   ├── StoresPage.tsx
│   ├── ChunksPage.tsx
│   └── SettingsPage.tsx
├── styles/
│   ├── reset.css
│   ├── theme.css           # CSS variables
│   └── components/         # Component-specific styles
├── lib/
│   ├── query-parser.ts     # Query syntax parsing
│   ├── formatters.ts       # Date, byte formatting
│   └── keyboard.ts         # Keyboard shortcut handling
├── App.tsx
└── main.tsx
```

## Priority Order

1. **Phase 1.1** - API Integration (required for everything else)
2. **Phase 2.2** - Search Execution (core functionality)
3. **Phase 2.3** - Results Display (see actual data)
4. **Phase 4.1** - Store List (navigation)
5. **Phase 3** - Live Tail (key differentiator)
6. **Phase 5.1** - Theme System (fix current issues)
7. Everything else...
