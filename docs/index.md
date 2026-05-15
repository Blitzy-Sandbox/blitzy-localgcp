# localgcp — LocalStack for GCP

Run Google Cloud locally. One Go binary emulates fourteen GCP services on localhost: Vertex AI, BigQuery, Spanner, Firestore, Pub/Sub, Cloud Storage, Bigtable, Cloud SQL, Memorystore, Cloud Tasks, Cloud KMS, Secret Manager, Cloud Run, and Cloud Logging. Zero cloud bills, zero API keys, works offline.

localgcp is the open-source GCP emulator — the LocalStack equivalent for Google Cloud. Your existing GCP client libraries (Go, Python, Java, Node.js) work unchanged via the standard `*_EMULATOR_HOST` environment variables.

## Why Vertex AI locally?

Every Vertex AI API call costs money. Every prompt iteration, every integration test, every debug session. localgcp lets you run your GCP AI code against Gemma, Llama, or any Ollama model running on your machine. The official `google.golang.org/genai` SDK works unchanged — just set the `BaseURL` to localgcp. No API keys, no quotas, no bills.

Without Ollama running, localgcp returns deterministic stub responses, perfect for CI/CD pipelines that need to test Vertex AI integration code without burning credits or leaking API keys.

## Use cases

- Local development — fast iteration without cloud bills or network latency.
- CI/CD testing — ephemeral emulator starts in milliseconds, no cloud credentials needed.
- Offline development — works without internet access.
- Integration testing — test against real GCP client libraries, not mocks.

## Where to next

- See **Setup** for install and quick-start instructions.
- See **Overview** for the emulated service catalog and project layout.
