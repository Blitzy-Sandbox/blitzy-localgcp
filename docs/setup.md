# Setup

## Install

localgcp ships as a single Go binary. Pick whichever install path matches your environment.

```bash
brew install slokam-ai/tap/localgcp
go install github.com/slokam-ai/localgcp/cmd/localgcp@latest
docker run --rm \
  -p 4443:4443 -p 8085:8085 -p 8086:8086 -p 8088:8088 \
  -p 8089:8089 -p 8090:8090 -p 8091:8091 -p 8092:8092 -p 8093:8093 \
  ghcr.io/slokam-ai/localgcp
```

Pre-built binaries are also available from the GitHub releases page.

## Quick start

### 1. Start the emulator

```bash
localgcp up
```

All native services start in the foreground. Data lives in memory and vanishes when you stop. Press Ctrl+C to stop.

For persistent data: `localgcp up --data-dir=./.localgcp`. To include Docker-orchestrated services (Spanner, Bigtable, Cloud SQL, Memorystore): `localgcp up --services=spanner,bigtable`.

### 2. Point your app at it

```bash
eval $(localgcp env)
```

This sets the `*_EMULATOR_HOST` environment variables. Existing GCP client code works with zero changes for Cloud Storage, Pub/Sub, and Firestore. Secret Manager has no official emulator host env var — configure the endpoint manually. Vertex AI uses the `google.golang.org/genai` SDK with `HTTPOptions.BaseURL` pointing at `http://localhost:8090`.

### 3. Run your application

GCP client libraries now talk to localhost instead of Google Cloud.

## CLI reference

```
localgcp up [flags]        Start all services (foreground)
localgcp env [flags]       Print export statements for client libraries
localgcp pull [flags]      Pre-fetch Docker images for orchestrated services
localgcp --version         Print version
```

Common flags: `--data-dir`, per-service `--port-*` overrides, `--services`, `--no-docker`, `--ollama-host`, `--vertex-backend`, `--quiet`.

## Development setup

Requires Go 1.22+ (developed on 1.26).

```bash
git clone https://github.com/slokam-ai/localgcp.git
cd localgcp
go build ./...
go test ./...
go run ./examples/smoketest/
```
