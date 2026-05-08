# Kubernetes

Deploy GastroLog as a StatefulSet on Kubernetes (production K8s,
OrbStack, kind, minikube, EKS, GKE, AKS — anywhere).

## Prerequisites

- A Kubernetes cluster you can `kubectl apply` to.
- The `gastrolog` image available to the cluster — either pulled
  from a registry or loaded into the cluster's image cache.
  - **OrbStack**: `docker build -t gastrolog:test .` makes the image
    visible to OrbStack's K8s automatically.
  - **kind**: `kind load docker-image gastrolog:test`.
  - **minikube**: `minikube image load gastrolog:test`.

## Quick start

```sh
kubectl apply -f k8s/statefulset.yml
kubectl -n gastrolog get pods -w
# wait for gastrolog-bootstrap-0, gastrolog-joiner-0, gastrolog-joiner-1 all Running

# OrbStack/kind: NodePort exposes 4564 on localhost:30564
open http://localhost:30564          # admin / change-me-please

# Or port-forward:
kubectl -n gastrolog port-forward svc/gastrolog 4564:4564
```

To tear down:

```sh
kubectl delete -f k8s/statefulset.yml
# PVCs are not auto-deleted; remove them explicitly if you want to wipe data:
kubectl -n gastrolog delete pvc --all
```

## What's in the recipe

The manifest ([`k8s/statefulset.yml`](../../k8s/statefulset.yml))
defines:

- **Namespace** `gastrolog` — keeps everything scoped.
- **Headless Service** `gastrolog-headless` — `clusterIP: None` so
  per-pod DNS records resolve to actual pod IPs (required for
  Raft).
- **NodePort Service** `gastrolog` — external ingress on port
  30564 → 4564.
- **ConfigMap** `gastrolog-config` — non-secret env (listen
  address, cluster-addr=`auto`, the join-addr DNS for joiners).
- **Secret** `gastrolog-secrets` — initial admin credentials and
  the bootstrap-token shared secret.
- **Bootstrap StatefulSet** `gastrolog-bootstrap` — `replicas: 1`,
  generates cluster TLS + join token, serves the token via
  `/cluster/bootstrap-token`.
- **Joiner StatefulSet** `gastrolog-joiner` — `replicas: 2`,
  polls the bootstrap pod's HTTP token endpoint and enrolls.

### Architecture

```
                    Namespace: gastrolog
   ┌──────────────────────────────────────────────────────────┐
   │                                                           │
   │  gastrolog-bootstrap-0  ◄─raft─►  gastrolog-joiner-0      │
   │       (StatefulSet,                  (StatefulSet,        │
   │        replicas: 1)                   replicas: 2)        │
   │                                                           │
   │                          ◄─raft─►  gastrolog-joiner-1     │
   │                                                           │
   │  Stable per-pod DNS via gastrolog-headless:                │
   │    gastrolog-bootstrap-0.gastrolog-headless...            │
   │    gastrolog-joiner-0.gastrolog-headless...               │
   │    gastrolog-joiner-1.gastrolog-headless...               │
   │                                                           │
   │  ────────  External  ────────                             │
   │  NodePort 30564 → 4564 (HTTP/Connect-RPC)                 │
   └──────────────────────────────────────────────────────────┘
```

### Why two StatefulSets instead of one with replicas=N?

The cleanest role differentiation. The bootstrap pod has a
distinct startup path (generate cluster TLS, serve the token);
joiners have a different one (poll the token, enroll). With one
StatefulSet, the pod-0-is-special logic has to live in an init
container that parses the pod's ordinal — ugly.

Two StatefulSets makes each role's env vars explicit and visible
in `kubectl describe`. Each StatefulSet's spec tells the operator
exactly what that pod set does without reading any branching shell
script.

### Why HTTP token delivery instead of file-based?

K8s pods don't natively share volumes between replicas. The
file-based path (works in Compose and Swarm via shared named
volumes) requires a `ReadWriteMany` storage class — uncommon,
not portable to all K8s distros.

The HTTP endpoint path (`gastrolog-o9z6o`) is the natural K8s fit:
the bootstrap pod serves the token at a known URL, joiners poll
with backoff. The shared secret in the K8s Secret authenticates
the request.

If you want the file-based path on a K8s with `ReadWriteMany`
support (Rook/Ceph, AWS EFS, Azure Files, NFS), you can adapt
the manifest by adding a shared PVC mounted into both
StatefulSets at `/shared` and switching to
`GASTROLOG_WRITE_BOOTSTRAP_TOKEN` / `GASTROLOG_BOOTSTRAP_TOKEN_FILE`
env vars.

### How the cluster forms

1. K8s schedules `gastrolog-bootstrap-0`. Its entrypoint sees
   `GASTROLOG_CLUSTER_ADDR=auto` and resolves the pod's own IP
   via `hostname -i` (e.g. `10.244.1.17`). Binds + advertises on
   that IP:4566.
2. Bootstrap generates cluster TLS + join token, mounts the
   admin credentials from `/etc/gastrolog/admin_creds`, provisions
   the initial admin user, and starts serving:
   - `/healthz`, `/readyz` for K8s probes
   - `/cluster/bootstrap-token` gated on the
     `GASTROLOG_BOOTSTRAP_TOKEN_SERVE_SECRET`
   - The full Connect-RPC + UI on port 4564
3. K8s schedules joiner pods (`gastrolog-joiner-0`, then `-1`).
   Each polls
   `http://gastrolog-bootstrap-0.gastrolog-headless.gastrolog.svc.cluster.local:4564/cluster/bootstrap-token`
   with the shared secret in the `X-Bootstrap-Token-Secret` header.
4. Joiner receives the token, enrolls with bootstrap via the
   stable per-pod DNS (`gastrolog-bootstrap-0.gastrolog-headless...:4566`),
   joins the Raft cluster.
5. Both joiners settle as `FOLLOWER` / `VOTER` within ~10
   seconds.

### Probes

The manifest wires K8s `livenessProbe` and `readinessProbe` to
the corresponding HTTP endpoints. K8s probes override the image's
built-in `HEALTHCHECK` — the `Dockerfile` directive is for
plain-Docker users.

- **Liveness** (`/healthz`): if it fails, K8s restarts the pod.
- **Readiness** (`/readyz`): if it fails, K8s removes the pod
  from Service endpoints. Useful during startup, leader-loss
  windows, and graceful shutdown.

## Verify

```sh
kubectl -n gastrolog get pods
# NAME                    READY   STATUS    RESTARTS   AGE
# gastrolog-bootstrap-0   1/1     Running   0          2m
# gastrolog-joiner-0      1/1     Running   0          1m
# gastrolog-joiner-1      1/1     Running   0          1m

# Cluster topology from inside the bootstrap:
kubectl -n gastrolog exec gastrolog-bootstrap-0 -- /gastrolog --home /config cluster status

# Admin login (via NodePort or port-forward):
curl -s -X POST -H "Content-Type: application/json" \
  http://localhost:30564/gastrolog.v1.AuthService/Login \
  -d '{"username":"admin","password":"change-me-please"}'
```

## Scaling out

Add joiners with:

```sh
kubectl -n gastrolog scale statefulset gastrolog-joiner --replicas=4
```

K8s creates `gastrolog-joiner-2` and `gastrolog-joiner-3`; each
gets its own PVC via the `volumeClaimTemplates`, polls the
bootstrap pod for the token, and enrolls.

The bootstrap StatefulSet should stay at `replicas: 1` — there's
exactly one bootstrap node per cluster lifetime.

## Production considerations

- **Image registry**: change `image: gastrolog:test` to a
  registry-pulled image (`ghcr.io/kluzzebass/gastrolog:v1.2.3`).
  Add `imagePullSecrets` if you need registry auth.
- **Resource limits**: add `resources.requests` and
  `resources.limits` to each container for CPU/memory.
- **Secrets**: replace the in-line `stringData` with sealed-secrets,
  external-secrets-operator, or `kubectl create secret` from a
  file that's never checked in. **Always rotate the
  `bootstrap_token_secret` after first use.**
- **Storage class**: the `volumeClaimTemplates` use the cluster's
  default storage class. For production, set
  `storageClassName: <your-class>` explicitly so PVCs land on
  the right backend (SSD, NVMe, network-attached, etc.).
- **TLS**: this manifest exposes plaintext HTTP via NodePort.
  In production, use an Ingress with TLS termination
  (cert-manager + Let's Encrypt is the common path on K8s).
- **PodDisruptionBudget**: add one for `gastrolog-joiner` so
  cluster maintenance doesn't take down enough nodes to lose
  Raft quorum.
- **Network policies**: restrict 4566 to intra-cluster traffic
  (`from: app=gastrolog`); 4564 stays open for the dashboard.
- **Backup**: snapshot the PVCs via your cluster's volume
  snapshotter (`VolumeSnapshot` resource, or storage-class-specific
  tooling).

## Troubleshooting

### Pod stuck in `Init` or `ContainerCreating`

Usually a missing image. Verify:

```sh
kubectl -n gastrolog describe pod gastrolog-bootstrap-0 | tail -20
```

For OrbStack, kind, or minikube, the image must be loaded into
the cluster's local image cache (see Prerequisites). For a
real cluster with `imagePullPolicy: IfNotPresent`, the image must
be available at the configured registry.

### Bootstrap pod ready but joiners stuck

Joiners can't reach the bootstrap's HTTP endpoint. Check:

```sh
kubectl -n gastrolog logs gastrolog-joiner-0 | grep -i bootstrap
```

If you see "endpoint rejected secret" 401s, the
`bootstrap_token_secret` doesn't match between the bootstrap's
`GASTROLOG_BOOTSTRAP_TOKEN_SERVE_SECRET` and the joiner's
`GASTROLOG_BOOTSTRAP_TOKEN_SECRET`. Both reference the same
Secret key, so this means the Secret was rotated mid-run; restart
the bootstrap pod to pick up the new value.

### `/readyz` returns 503

The orchestrator is still catching up after a restart, or local
vault FSMs haven't applied any log entries yet. Wait 10-30
seconds. If readiness doesn't recover, check the FSM logs:

```sh
kubectl -n gastrolog logs gastrolog-bootstrap-0 | grep -iE 'fsm|catch'
```
