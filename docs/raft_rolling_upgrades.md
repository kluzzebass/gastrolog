Rolling Upgrade Invariants and Algorithm
========================================

Status: built.

- Rolling restarts work, and protobuf encoding keeps added fields compatible
  across binary versions (I1–I3).
- Command gating (I4) and the monotonic active protocol level (I5) hold for
  the cluster-ctl group and every vault-ctl group. The code is in
  `backend/internal/raftlevel`.
- A node that still meets a Raft entry it cannot apply stops the process
  instead of skipping the entry.

The release that introduces the gate cannot protect itself: its own commands
are new to the binaries before it, which have no gate. Moving a cluster to it
takes a full-cluster restart or a reinitialization. Every release after it
upgrades one node at a time.


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

I4. Command Gating
    A command type is not proposed until every member of the cluster-ctl
    Raft configuration runs a binary that knows it.

I5. Monotonic Active Protocol Level
    The active protocol level:
      - Is stored in Raft (cluster-ctl)
      - Only ever increases
      - Is raised only after every member supports it

I6. Deterministic FSM
    Command application depends only on:
      - Command payload
      - Current FSM state
    Never on:
      - Binary version
      - Local flags
      - Environment


The Rule
--------

New behaviour is a new command type. An existing command's apply semantics
never change.

A command type therefore means the same thing to every binary that knows it,
and the level table — which maps each command type to the protocol level that
introduced it — stays complete: a binary that does not know a command type
never sees it while the gate holds.

Protobuf rules for the commands themselves:
- Never reuse a oneof field number.
- Only add new command types and optional fields.
- Never remove a field in a way that breaks an older reader.


Protocol Levels
---------------

Binary level
    `raftlevel.Binary`, a constant in the binary: the highest command level
    it can apply. A release that adds a command type raises it.

Level table
    `raftlevel.DefaultTable` maps every oneof case of `SystemCommand`
    (cluster-ctl), `VaultRaftCommand` and `VaultCtlCommand` (vault-ctl) to
    the level that introduced it. A test enumerates the oneof cases through
    protobuf reflection and fails if any case has no entry, or an entry above
    `Binary`.

Reported level
    Each node reports its binary's level in its cluster-ctl node record
    (`NodeConfig.protocol_level`), written by the node's startup
    `ensureNodeConfig`. A placeholder record the leader writes for a joining
    node carries no level until the node writes its own.

Active level
    The cluster's committed level, in cluster-ctl state
    (`system.Runtime.ProtocolLevel`, snapshot field `protocol_level`),
    committed by `SetProtocolLevelCommand`. The apply refuses a level below
    the current one. A cluster that has not committed one runs at
    `raftlevel.Base`.


Raising the Active Level
------------------------

`raftlevel.Raiser` runs on every node and acts only on the cluster-ctl
leader. It evaluates when a node record is put or deleted (the config
dispatcher wakes it from the FSM notification) and when leadership changes.
No timer drives it.

    members  = every server in the cluster-ctl Raft configuration,
               voters and learners alike (a learner applies the log too)
    target   = the lowest level any member reports
    raise to target when:
        every member has a node record with a reported level, and
        target > active level


Proposer Gate
-------------

Every command passes `raftlevel.Gate` before it reaches `raft.Apply`:

- cluster-ctl: `raftstore.Store.applyRaw`, the one path every cluster-ctl
  command takes, on the proposing node and again on the leader when a
  follower forwards it (`ForwardApply` calls the leader's `ApplyRaw`).
- vault-ctl: `Orchestrator.ApplyVaultControlPlane` and the vault chunk
  applier (`gatedChunkApplier`) on the proposing node, and the
  `ForwardVaultApply` handler on the leader before it proposes a forwarded
  entry.

The gate refuses:
- a command whose level is above the active level
  (`ErrCommandAboveClusterLevel`), naming both levels;
- on the node that hands the entry to `raft.Apply`, an entry it cannot decode
  itself (`ErrUndecodableCommand`) or whose command type it does not know
  (`ErrUnknownCommand`). This covers a newer node forwarding a new command
  type to an older leader.

A forwarded refusal comes back to the follower as a rejected command
(`FailedPrecondition` for cluster-ctl, `InvalidArgument` for vault-ctl, so
the vault forwarder does not retry it against the next leader).


Apply: Fail-Stop
----------------

hashicorp/raft treats an entry as applied whatever `FSM.Apply` returns, and
an `Apply` that never returns hangs Raft shutdown. So an FSM that meets an
entry it cannot apply — undecodable, of an unknown command type (at the
vault-ctl envelope or the vault-scoped inner command), or raising the active
level above its binary's — calls its unapplicable hook. The production hook,
`raftlevel.FailStop`, logs the group, index and reason and exits the
process. The FSM neither applies the entry nor advances its applied index
past it. The node crash-loops with that reason until it runs a binary that
can apply the entry; its peers see it unreachable.

A known command the FSM refuses on its merits (a seal of an unknown chunk, a
lowering of the active level) is not unapplicable: every replica refuses it
the same way, so the FSM returns the error and carries on.


Startup
-------

A binary below the committed active level refuses to start. The check lives
in the cluster-ctl FSM: applying a `SetProtocolLevelCommand`, or restoring a
snapshot, whose level is above the binary's is unapplicable, and the node
stops with an error naming both levels. The FSM is the one place that sees
the level however the node starts — replaying its own log after a restart,
installing a snapshot from the leader, or catching up through the startup
barrier — and it stops the node before it applies anything the level
admitted.


Rolling Upgrade Algorithm
-------------------------

Precondition:
  Every node runs a binary at level L; the active level is L.

Goal:
  Upgrade to a binary at level L+1 that adds a command type.

Phase 1: Deploy new binaries, one node at a time
    1. Stop the node.
    2. Install the new binary.
    3. Restart the node. It reports level L+1 in its node record.
    4. Wait for the cluster to be healthy (quorum intact) before the next.

  During Phase 1 the active level stays L. The level-(L+1) command type is
  refused at every proposer, so the cluster behaves as the old version.

Phase 2: Activation
  When the last member reports L+1, the cluster-ctl leader commits
  `SetProtocolLevel(L+1)`. Every binary applies it: it is a level-1 command.

Phase 3: New behaviour
  Nodes propose the level-(L+1) command type. No member is incapable of
  applying it.


Failure Handling
----------------

A node that comes back on an older binary after the level was raised stops
when its FSM reaches the raise (or a snapshot that contains it), before it
applies anything the raise admitted. Upgrade its binary.

A node that is down for the whole upgrade keeps the active level at L: it is
still a member and still reports L. Upgrade it, or remove it from the
cluster, before the new command type becomes usable.


Summary of the Critical Rule
----------------------------

    Upgrade binaries first.
    Raise the active level second.

Never reverse this order; the gate enforces it.

Binary compatibility protects log replay.
Command gating protects mixed-version clusters.
A monotonic active level prevents downgrade corruption.
