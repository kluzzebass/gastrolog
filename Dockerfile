# Stage 1: Build frontend
FROM oven/bun:1 AS frontend
WORKDIR /src/frontend
COPY frontend/package.json frontend/bun.lock* ./
RUN bun install --frozen-lockfile
COPY frontend/ .
RUN bun run build

# Stage 2: Build backend (with embedded frontend)
FROM golang:1.26 AS backend
WORKDIR /src/backend
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ .
COPY --from=frontend /src/frontend/dist/ internal/frontend/dist/
RUN go run ./cmd/compress-assets internal/frontend/dist
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags "-X main.version=${VERSION}" -o /gastrolog ./cmd/gastrolog

# Stage 3: Runtime
# busybox (~4 MB) instead of scratch — enables entrypoint scripts, exec,
# and shell-based orchestration for cluster bootstrapping.
FROM busybox:1.37-musl

# OCI image labels (https://github.com/opencontainers/image-spec/blob/main/annotations.md).
# Consumers like GHCR, Docker Hub, and Artifact Registry use these to
# render image metadata. VERSION comes from the build arg; the rest
# are static.
ARG VERSION=dev
LABEL org.opencontainers.image.title="GastroLog" \
      org.opencontainers.image.description="Distributed log aggregation service with built-in clustering, routing, and tiered storage." \
      org.opencontainers.image.source="https://github.com/kluzzebass/gastrolog" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=backend /gastrolog /gastrolog
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Persistent data directories. Orchestrators typically mount their own
# volumes here (PVCs, named volumes, host bind-mounts); the VOLUME
# declarations document the contract — anything written to these paths
# must be persistent across container restarts. Defaults match the
# entrypoint's GASTROLOG_HOME / GASTROLOG_VAULTS values.
VOLUME ["/config", "/vaults"]

# 4564 — HTTP / Connect-RPC server (operator + ingestion API).
# 4566 — cluster gRPC (inter-node Raft + RPC). Required for multi-node
# deployments; harmless to expose for single-node.
EXPOSE 4564 4566

# Liveness probe — /healthz returns 200 if the HTTP listener is serving.
# Docker uses this to decide whether to mark the container unhealthy and
# (with a restart policy) restart it. K8s users typically configure their
# own probes via the Pod spec and override this.
#
# /readyz is also exposed (200 when the orchestrator is running, not
# draining, and local vault FSMs are caught up) but isn't used here:
# Docker has no separate readiness concept, so liveness-only is the
# right shape for this directive.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:4564/healthz || exit 1

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["server"]
