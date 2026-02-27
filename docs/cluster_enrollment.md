Cluster Node Enrollment and Certificate Exchange
=================================================

Overview
--------

Nodes join a cluster using a one-time token exchange over a dedicated
cluster port. After enrollment, all nodes share the same TLS certificate
for mutual authentication. The certificate is replicated via the Raft
config FSM.

Prior art: K3s bootstrap model.


Network Architecture
--------------------

Two ports per node:

    HTTPS port    Browser UI, API, Connect RPC. Proxyable.
    Cluster port  gRPC with mTLS. Raft, enrollment, inter-node RPCs.

The cluster port runs a gRPC server with mTLS. Raft transport uses
Jille/raft-grpc-transport (Raft RPCs as gRPC services). Additional
gRPC services on the same port handle inter-node operations such as
chunk transfer, federated query forwarding, and health checks.


Cluster Bootstrap (First Node)
------------------------------

First node starts with --bootstrap. Generates:

    1. Cluster CA        Self-signed X.509 CA key pair.
    2. Cluster cert       Signed by the CA.
                         ExtKeyUsage: ServerAuth + ClientAuth.
    3. Join token        Random secret.

Token secure format:

    <secret>:<sha256 of CA cert>

The CA cert, cluster cert+key, and join token are stored in the
config FSM. The node begins listening on the cluster port with
the cluster cert.


Node Enrollment
---------------

New node starts with:

    gastrolog server \
      --join-token <secret>:<ca-hash> \
      --join-addr <leader-cluster-addr>

Sequence:

    1. Node connects to leader's cluster port over TLS.
    2. Node verifies leader's cert against the CA hash
       embedded in the token. Prevents MITM.
    3. Node sends the secret portion to prove authorization.
    4. Leader verifies the token.
    5. Leader calls AddVoter or AddNonvoter.
    6. Raft replicates config FSM to the new node,
       including: cluster CA cert, cluster cert+key.
    7. Node loads the cluster cert from config.
    8. Node starts its own cluster port gRPC server with mTLS.

Enrollment is always node -> leader. The new node has nothing
to listen with before it receives the cluster cert.


Join Token Lifecycle
--------------------

- One reusable cluster token generated at bootstrap.
- Optional short-lived tokens via API (single-use or TTL-limited).
- Token is only used during enrollment. The shared cert is the
  ongoing credential.


Certificate Properties
----------------------

All nodes share the same certificate from the config store.

    Issuer:       Cluster CA (self-signed)
    Subject:      Cluster-specific CN
    ExtKeyUsage:  ServerAuth, ClientAuth
    Validity:     Long-lived (e.g. 10 years), rotatable

No per-node certificates. No CA signing/issuance capability
beyond the initial bootstrap.


Certificate Rotation
--------------------

    1. Operator (or automation) generates a new cluster cert
       signed by the same CA.
    2. New cert is written to config store.
    3. Raft replicates to all nodes.
    4. Nodes hot-reload the gRPC server TLS config.

CA rotation follows the same pattern but requires a two-phase
rollout: distribute the new CA cert first (so all nodes trust it),
then rotate the cluster cert to one signed by the new CA.


Node Revocation
---------------

Rotate the cluster cert. The evicted node does not receive the
config update (it has been removed from Raft membership) and
can no longer authenticate.


Constraints and Trade-offs
--------------------------

- Shared cert means a compromised node leaks the credential for
  the whole cluster. Acceptable for small clusters. Mitigated by
  cert rotation on eviction.

- No leader -> node enrollment. The leader cannot initiate a
  connection to a node that has no cert yet.

- Cluster port must be directly reachable between nodes. Cannot
  sit behind a TLS-terminating reverse proxy (mTLS requires
  end-to-end TLS).

- The HTTPS port remains independently configurable and proxyable.
