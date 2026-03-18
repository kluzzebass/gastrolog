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
COPY --from=backend /gastrolog /gastrolog
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
EXPOSE 4564
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["server"]
