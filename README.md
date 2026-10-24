# Cloud Tasks emulator

A local emulator of the Google Cloud Tasks v2 API. It serves every v2 RPC over gRPC and REST on one port. It pushes tasks to your handlers with the headers, retries and rate limits that Cloud Tasks applies. Code using the official client libraries or `gcloud` runs against it with no GCP project.

We test the emulator against transcripts recorded from real Cloud Tasks, so its responses and error messages match the real service.

## Run

```sh
LISTEN_ADDR=127.0.0.1:8123 OIDC_ISSUER=http://127.0.0.1:8123 QUEUES= cargo run --release
```

| Key | Value |
| --- | --- |
| `LISTEN_ADDR` | Address that serves gRPC, REST and OIDC discovery, for example `127.0.0.1:8123` |
| `OIDC_ISSUER` | Origin your handlers reach the emulator at, which signs OIDC tokens as this issuer |
| `QUEUES` | Full queue names separated by commas, created at startup with the default config. Empty for none |
| `APP_ENGINE_URL` | Origin that every App Engine task goes to. Unset when no project has an App Engine app |

## Connect

Point a client at `LISTEN_ADDR` without TLS. The emulator accepts any credentials, project ID and location ID.

```sh
# gcloud
export CLOUDSDK_API_ENDPOINT_OVERRIDES_CLOUDTASKS=http://127.0.0.1:8123/

# REST
curl http://127.0.0.1:8123/v2/projects/p/locations/l/queues
```

## Handlers

- HTTP tasks go to their URL with the `X-CloudTasks-*` headers. App Engine tasks go to `APP_ENGINE_URL` with the `X-AppEngine-*` headers.
- The emulator signs OIDC tokens with a key made each run. Handlers verify them through `<OIDC_ISSUER>/.well-known/openid-configuration`.
- OAuth tokens are opaque, as only Google APIs could check a real one.
- Unlike Cloud Tasks, the emulator sends tokens to http URLs, so local handlers receive them.

## Test helpers

- `QUEUES` creates queues at startup, so tests don't need to.
- `GET /emulator/<queue name>:waitIdle` blocks until the queue holds no tasks, then returns 204. It returns 404 for an unknown queue and 409 for a paused queue with tasks left.

```sh
curl http://127.0.0.1:8123/emulator/projects/p/locations/l/queues/q:waitIdle
```

## Differences from Cloud Tasks

- State lives in memory, so every start is a clean slate.
- The emulator does not check that a project, location or service account exists. It stores IAM policies without enforcing them.
- It stores CMEK keys without encrypting anything.
- Every App Engine task goes to one origin. Routing decides only the host the task reports.
- It serves v2 only, without the Locations RPCs.
- Bare words and text comparisons other than `:` in the `ListQueues` filter return `UNIMPLEMENTED`.
- REST rejects partial responses through the `fields` parameter.

## Develop

Check every change with `cargo test`, `cargo clippy --all-targets` and `cargo fmt --check`. Building needs no `protoc`.

`cargo test` compares the emulator with the transcripts in `tests/recordings`. To record them again, install `cloudflared` and run:

```sh
CLOUD_TASKS_PROJECT=<id> CLOUD_TASKS_LOCATION=<id> CLOUD_TASKS_SERVICE_ACCOUNT=<email> CLOUD_TASKS_TOKEN=$(gcloud auth print-access-token) cargo test --test conformance -- --ignored record
```

Record in a sandbox project, since a run creates and deletes queues there. The service account must be in that project, and you need permission to act as it.
