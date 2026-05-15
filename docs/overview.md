# Overview

localgcp is a single Go binary that emulates GCP services on localhost. Nine services are implemented natively in Go with no runtime dependencies. Five additional services are orchestrated via Docker containers that start lazily on first use. Native service data is stored in memory by default, with optional JSON-file persistence via `--data-dir`.

GCP client libraries already support `*_EMULATOR_HOST` environment variables. When set, the libraries connect to localhost instead of Google Cloud. localgcp uses this mechanism for zero-friction SDK compatibility.

## Emulated services

| Service | Protocol | Port | Env var |
|---------|----------|------|---------|
| Cloud Storage | REST | 4443 | `STORAGE_EMULATOR_HOST` |
| Pub/Sub | gRPC | 8085 | `PUBSUB_EMULATOR_HOST` |
| Secret Manager | gRPC | 8086 | (manual endpoint config) |
| Firestore | gRPC | 8088 | `FIRESTORE_EMULATOR_HOST` |
| Cloud Tasks | gRPC | 8089 | (manual endpoint config) |
| Vertex AI | REST | 8090 | (manual endpoint config) |
| Cloud KMS | gRPC | 8091 | (manual endpoint config) |
| Cloud Logging | gRPC | 8092 | (manual endpoint config) |
| Cloud Run | gRPC | 8093 | (manual endpoint config) |
| Spanner (Docker) | gRPC | 9010 | `SPANNER_EMULATOR_HOST` |
| Bigtable (Docker) | gRPC | 9094 | `BIGTABLE_EMULATOR_HOST` |
| Cloud SQL (Docker) | TCP | 5432 | (standard Postgres) |
| Memorystore (Docker) | TCP | 6379 | (standard Redis) |
| BigQuery (Docker, LocalBQ) | REST | 9060 | `CLOUDSDK_API_ENDPOINT_OVERRIDES_BIGQUERY` |

## Project structure

```
cmd/localgcp/              CLI entry point (cobra)
internal/server/           Multi-service lifecycle, shutdown, port management
internal/auth/             Dummy credential generation
internal/gcs/              Cloud Storage emulator (REST/HTTP)
internal/pubsub/           Pub/Sub emulator (gRPC)
internal/secretmanager/    Secret Manager emulator (gRPC)
internal/firestore/        Firestore emulator (gRPC, including query engine)
internal/cloudtasks/       Cloud Tasks emulator (gRPC)
internal/vertexai/         Vertex AI emulator (REST; Ollama/OpenAI/Anthropic backends)
internal/kms/              Cloud KMS emulator (gRPC)
internal/logging/          Cloud Logging emulator (gRPC)
internal/cloudrun/         Cloud Run emulator (gRPC)
internal/orchestrator/     Docker orchestrator for Spanner, Bigtable, Cloud SQL, Memorystore
internal/dispatch/         Shared HTTP dispatcher with retry (used by Cloud Tasks)
examples/smoketest/        SDK integration test using official GCP client libraries
examples/vertexai/         Vertex AI SDK example
website/                   Landing page + 14 documentation pages (static HTML)
```

Each native service uses `store.go` (in-memory model + JSON persistence), `service.go` (HTTP or gRPC server), and `service_test.go`. The orchestrator uses a `ContainerRuntime` interface plus a `LazyService` TCP proxy that starts containers on first connection.

## Out of scope (today)

Not yet implemented: bucket versioning, exactly-once Pub/Sub delivery, Firestore composite indexes, Cloud Tasks OIDC/OAuth auth, Vertex AI multimodal, Secret Manager IAM and rotation, Cloud KMS key import and rotation schedules, Cloud Logging sinks and log-based metrics, Cloud Run container execution and traffic splitting, persistence for orchestrated services, IAM/auth enforcement.
