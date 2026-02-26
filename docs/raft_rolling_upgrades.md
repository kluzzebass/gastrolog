Rolling Upgrade Invariants and Algorithm
========================================

Core Safety Invariants
----------------------

I1. Log Compatibility
    Every committed log entry must be:
      - Parseable by all running nodes
      - Deterministically applicable by all running nodes

I2. Backward Compatibility
    New binaries must:
      - Understand all previously committed command formats
      - Correctly apply old log entries

I3. Forward Safety
    Old binaries must:
      - Safely ignore unknown optional fields
      - Never encounter a command type they cannot apply

I4. Feature Gating
    New command types or semantics MUST NOT be emitted
    until all voting members support them.

I5. Monotonic Feature Level
    cluster.feature_level:
      - Stored in Raft
      - Monotonically increasing
      - Raised only after all voters support it

I6. Deterministic FSM
    Command application must depend only on:
      - Command payload
      - Current FSM state
    Never on:
      - Binary version
      - Local flags
      - Environment


Cluster State Model
-------------------

cluster_state {
    binary_min_supported_version
    feature_level
}

node_state {
    binary_version
    supported_feature_level
}


Command Encoding Rules
----------------------

Command {
    type            // never reuse
    version         // schema version
    payload         // forward-compatible encoding (e.g. protobuf)
}

Rules:
- Never change meaning of an existing command type.
- Only add new command types.
- Only add optional fields.
- Never remove fields in a way that breaks old readers.


Rolling Upgrade Algorithm
-------------------------

Precondition:
  All nodes running version V.
  cluster.feature_level = F.

Goal:
  Upgrade to version V+1 with new feature requiring feature_level F+1.


Phase 1: Deploy New Binaries (Compatibility Phase)

for each node in cluster:
    1. Stop node.
    2. Install new binary (V+1).
    3. Restart node.
    4. Verify:
         node.binary_version == V+1
         node.supported_feature_level >= F
    5. Ensure cluster remains healthy (quorum intact).

Invariant during Phase 1:
  - cluster.feature_level remains F.
  - No new command types are emitted.
  - System behavior identical to old version.


Phase 2: Verify Cluster Homogeneity

Leader checks:
    for each voting member:
        if member.binary_version < V+1:
            abort upgrade

Only proceed when:
    all voters support feature_level >= F+1


Phase 3: Activate Feature (Consensus Phase)

Leader proposes Raft entry:

    SetClusterFeatureLevel(F+1)

This entry must:
    - Be understandable by old and new binaries
    - Be valid only if all nodes support F+1

Upon commit:
    cluster.feature_level = F+1


Phase 4: Enable New Behavior

After feature_level >= F+1:

    Nodes may now:
        - Emit new command types
        - Use new config fields
        - Enforce new validation rules

Invariant:
    No node in cluster is incapable of applying new commands.


Failure Handling
----------------

If a node with old binary rejoins after Phase 3:

    On startup:
        if node.supported_feature_level < cluster.feature_level:
            refuse to start
            require binary upgrade

Never allow:
    A node to participate in Raft if it cannot apply all
    committed log entries.


Summary of the Critical Rule
----------------------------

    Upgrade binaries first.
    Upgrade cluster semantics second.

Never reverse this order.

Binary compatibility protects log replay.
Feature gating protects mixed-version clusters.
Monotonic feature levels prevent downgrade corruption.
