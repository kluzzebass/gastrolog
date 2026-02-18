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
FROM scratch
COPY --from=backend /gastrolog /gastrolog
EXPOSE 4564
ENTRYPOINT ["/gastrolog"]
CMD ["server"]
