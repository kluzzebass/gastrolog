# Cluster Request Routing

How requests are routed between nodes in a GastroLog cluster. Every node
can serve any request. The routing layer determines which node should
handle it and forwards transparently.

## Routing Strategies

Every RPC handler declares one of four routing strategies:

```mermaid
flowchart TD
    Request["Incoming RPC"] --> Interceptor["Routing Interceptor"]

    Interceptor --> CheckStrategy{"Declared strategy?"}

    CheckStrategy -->|RouteLocal| Local["Execute locally"]
    CheckStrategy -->|RouteTargeted| FindOwner{"Vault owner = this node?"}
    CheckStrategy -->|RouteLeader| IsLeader{"This node is leader?"}
    CheckStrategy -->|RouteFanOut| FanOut["Execute on all nodes + merge"]
    CheckStrategy -->|Undeclared| Reject["Reject: missing strategy"]

    FindOwner -->|Yes| Local
    FindOwner -->|No| Forward["Forward to owner"]
    Forward --> ProxyBack["Proxy response back"]

    IsLeader -->|Yes| Local
    IsLeader -->|No| ForwardLeader["Forward to leader"]
    ForwardLeader --> ProxyBack
```

| Strategy | When to use | Example RPCs |
|----------|------------|-------------|
| **RouteLocal** | Any node can serve from local state (Raft-replicated config, peer broadcasts) | Health, WatchConfig, ListVaults, GetClusterStatus |
| **RouteTargeted** | Request is about a specific vault owned by one node | GetIndexes, ListChunks, SealVault, GetContext |
| **RouteFanOut** | Needs data from all nodes, merged at the coordinator | Search, GetFields, ComputeHistogram |
| **RouteLeader** | Must be applied through Raft consensus | PutVaultConfig, PutIngesterConfig, ForwardApply |

## Request Flow

### RouteTargeted (most common)

```mermaid
sequenceDiagram
    participant Client
    participant Node1 as Node 1 (API)
    participant Node2 as Node 2 (vault owner)

    Client->>Node1: GetIndexes(vault=V)
    Node1->>Node1: Interceptor: RouteTargeted
    Node1->>Node1: Lookup vault V owner
    alt Vault is local
        Node1->>Node1: Execute handler
        Node1-->>Client: Response
    else Vault is on Node 2
        Node1->>Node2: ForwardGetIndexes(vault=V)
        Node2->>Node2: Execute handler locally
        Node2-->>Node1: Response
        Node1-->>Client: Response (proxied)
    end
```

The client always talks to one node. Forwarding is invisible.

### RouteFanOut

```mermaid
sequenceDiagram
    participant Client
    participant Coord as Coordinator
    participant N2 as Node 2
    participant N3 as Node 3

    Client->>Coord: Search(query)
    Coord->>Coord: Local search (local vaults)
    Coord->>N2: ForwardSearch(query, vault_a)
    Coord->>N3: ForwardSearch(query, vault_b)
    N2-->>Coord: Stream records
    N3-->>Coord: Stream records
    Coord->>Coord: Merge-sort all streams
    Coord-->>Client: Merged results
```

Fan-out RPCs are always streaming. The coordinator merges results from
all nodes before sending to the client. The merge logic is
handler-specific (timestamp-ordered for search, union for fields).

### RouteLeader

```mermaid
sequenceDiagram
    participant Client
    participant Follower as Follower Node
    participant Leader as Leader Node

    Client->>Follower: PutVaultConfig(config)
    Follower->>Follower: Interceptor: RouteLeader
    Follower->>Follower: Am I leader? No
    Follower->>Leader: ForwardApply(config)
    Leader->>Leader: Raft.Apply()
    Leader-->>Follower: Applied
    Follower-->>Client: OK
```

Config mutations must go through Raft. If the receiving node is not the
leader, the interceptor forwards to the current leader automatically.

## Cluster Communication Channels

All inter-node communication runs over a single gRPC server per node
(cluster port, mTLS):

```mermaid
flowchart LR
    subgraph "Cluster gRPC Port (mTLS)"
        Raft["Raft Transport"]
        Admin["Raft Admin"]
        Health["Leader Health"]
        CS["ClusterService (23 RPCs)"]
    end

    subgraph "ClusterService RPCs"
        Unary["18 Unary RPCs"]
        Stream["5 Streaming RPCs"]
    end

    CS --> Unary
    CS --> Stream
```

### Unary RPCs (request-response)

| Category | RPCs |
|----------|------|
| Config | ForwardApply |
| Enrollment | Enroll (mTLS-exempt) |
| Stats | Broadcast |
| Ingestion | ForwardRecords |
| Inspector | ForwardListChunks, ForwardGetIndexes, ForwardGetChunk, ForwardAnalyzeChunk, ForwardValidateVault |
| Operations | ForwardSealVault, ForwardReindexVault, ForwardExportToVault |
| Context | ForwardGetContext |
| Membership | NotifyEviction, ForwardRemoveNode, ForwardSetNodeSuffrage |
| Files | ListPeerManagedFiles |

### Streaming RPCs

| RPC | Pattern | Purpose |
|-----|---------|---------|
| ForwardSearch | server-streaming | Search results from remote vaults |
| ForwardFollow | server-streaming | Live tail from remote vaults |
| ForwardImportRecords | client-streaming | Sealed chunk transfer |
| StreamForwardRecords | client-streaming | Bulk record ingestion (128 MB max) |
| PullManagedFile | server-streaming | File transfer between peers |

Streaming RPCs cannot be routed through a generic envelope because
their data flow is fundamentally different from request-response.

## Interceptor Enforcement

The routing interceptor provides two guarantees:

1. **Every RPC must declare a strategy.** Undeclared RPCs are rejected
   at startup. This prevents the "forgot to add routing" class of bugs.

2. **Targeted RPCs always reach the correct node.** The handler code
   never checks vault ownership — the interceptor handles it. If the
   vault is local, the handler runs. If remote, the request is
   forwarded and the response proxied back. The handler doesn't know
   the difference.

```mermaid
flowchart TD
    Startup["Server startup"] --> Scan["Scan all registered RPCs"]
    Scan --> Verify{"All have declared strategies?"}
    Verify -->|Yes| Ready["Server ready"]
    Verify -->|No| Fail["Refuse to start"]
```

## What This Does Not Cover

- **Client-side routing optimization.** The infrastructure routes
  correctly regardless of which node the client connects to. If the
  client happens to connect to the wrong node, there is one extra hop.
  Optimizing client-to-node affinity is a separate concern.

- **Load balancing.** For RouteLocal RPCs, the interceptor could route
  to the least loaded node using PeerState broadcast stats. This is a
  future optimization, not a correctness requirement.
