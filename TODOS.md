# TODOs

## Phase 2

### ~~Dispatcher: limit response body size~~ (DONE)
- Implemented in `internal/dispatch/dispatcher.go` with `io.LimitReader(resp.Body, 1<<20)`.

### ~~README: prior art section~~ (DONE)
- Added "Prior Art" section to README.md acknowledging fsouza/fake-gcs-server and aertje/cloud-tasks-emulator.

## Vertex AI Emulator

### ~~Streaming support (streamGenerateContent)~~ (DONE)
- Ollama NDJSON to Vertex JSON array streaming. Stub backend splits into word-level chunks.

### ~~Multi-provider support (OpenAI, Anthropic adapters)~~ (DONE)
- OpenAI and Anthropic backend adapters via `--vertex-backend` flag.
- Full streaming and tool/function calling support across all backends.

## Phase 3

### ~~Firestore Listen: resume tokens~~ (DONE)
- Implemented with global sequence counter, bounded ring buffer (1024 entries), and 8-byte resume tokens.
- Clients reconnecting with valid token get incremental changes; invalid/expired tokens fall back to full snapshot.

## Phase 4 — Docker Orchestrator

### ~~`localgcp pull` command~~ (DONE)
- `localgcp pull [--services=spanner,bigtable]` pre-fetches Docker images.
- Pulls all 4 images by default, or specific services via `--services` flag.

### ~~Data persistence for orchestrated containers~~ (DONE)
- `--data-dir` mounts host volumes into Docker containers for Cloud SQL and Memorystore.
- Redis gets `appendonly yes` mode when persisting. Postgres mounts `/var/lib/postgresql/data`.
- Spanner and Bigtable emulators don't support persistence (ephemeral only).

## Phase 3c — Cross-service wiring and Cloud Scheduler

### ~~Cloud Scheduler (native service)~~ (DONE)
- New native gRPC service on port 8094, registered with `schedulerpb.RegisterCloudSchedulerServer`.
- Eight in-scope RPCs: `CreateJob`, `GetJob`, `ListJobs`, `DeleteJob`, `UpdateJob`, `RunJob`, `PauseJob`, `ResumeJob`.
- Single `robfig/cron/v3` runner goroutine dispatches `HttpTarget` jobs via `internal/dispatch.Dispatcher` and `PubsubTarget` jobs via loopback Pub/Sub gRPC.
- `RunJob` performs an immediate single dispatch without mutating `schedule` or `state`.
- CLI flag `--port-cloud-scheduler` (default 8094) + `CLOUD_SCHEDULER_EMULATOR_HOST=localhost:8094` env export.

### ~~Cloud Run actual container execution~~ (DONE)
- `CreateService` allocates a host port from the 8200–8299 pool and registers the container image without starting.
- First HTTP request to the service URI triggers on-demand `CreateContainer` + `StartContainer` via `internal/orchestrator.ContainerRuntime`.
- `DeleteService` calls `StopContainer` + `RemoveContainer` and frees the port.
- Bounded pool: 100 concurrent services maximum, overflow returns `codes.ResourceExhausted`.
- `--no-docker` mode returns a non-empty stub URI without invoking Docker.
- Service URIs now return `http://localhost:{hostPort}` instead of synthetic `run.app` strings.

### ~~GCS → Pub/Sub notifications~~ (DONE)
- New `PUT/GET/DELETE /storage/v1/b/{bucket}/notificationConfigs` HTTP handlers.
- Object `PUT`/`POST` emits `OBJECT_FINALIZE` events; `DELETE` emits `OBJECT_DELETE` events.
- Canonical GCS JSON notification payload with `{eventType, bucketId}` attributes via loopback gRPC.
- Fire-and-forget goroutine model — GCS response latency unaffected by Pub/Sub publish.

### ~~Cloud Logging sinks~~ (DONE)
- Five new sink RPCs: `CreateSink`, `GetSink`, `UpdateSink`, `DeleteSink`, `ListSinks`.
- Destinations: `pubsub.googleapis.com/projects/{project}/topics/{topic}` or `storage.googleapis.com/{bucket}`.
- `WriteLogEntries` fans out matching entries to each sink via fire-and-forget goroutine.
- Delivery failures logged to stderr; `WriteLogEntries` caller never sees sink errors.


