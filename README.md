# Downloader SABnzbd

MuxCore sidecar that bridges [SABnzbd](https://sabnzbd.org/) for NZB/usenet downloads.

Exposes `muxcore.usenet.v1.UsenetDownloaderService` (AddNZB, queue, pause/resume/delete, history) plus SettingsProvider.

## Network safety / CI

- **Unit tests and CI never talk to a real SABnzbd or usenet provider.** They use an in-process `httptest` mock (`internal/sabnzbd.NewMockServer`) that exercises add → history → `download.completed`.
- Live SABnzbd is **operator opt-in only**: set `SABNZBD_URL` + `SABNZBD_API_KEY` (or admin settings). Leave them empty for soft-empty health (optional peer).
- Product gates and laptop demos do **not** require Usenet credentials.

## Offline / mock path

`OfflineDispatch` (and `AddNZB` with a mock base URL) queues an NZB against the configured HTTP API, polls history, and publishes:

| Event | When |
|-------|------|
| `download.started` | NZB accepted by SABnzbd |
| `download.completed` | History status contains `Complete` (payload includes `save_path` from SABnzbd `storage`) |
| `download.failed` | History status contains `Fail` |

When a publisher is injected (tests or a mesh adapter), events are emitted for:

| Event | When |
|-------|------|
| `download.started` | NZB accepted by SABnzbd |
| `download.completed` | History status contains `Complete` (payload includes `save_path` from SABnzbd `storage`) |
| `download.failed` | History status contains `Fail` |

CI injects a publisher sink; no live mesh is required for tests.

## Config

| Env | Setting | Default |
|-----|---------|---------|
| `SABNZBD_URL` | `base_url` | — (empty = unconfigured / soft-empty) |
| `SABNZBD_API_KEY` | `api_key` | — (operator opt-in) |
| gRPC listen | — | `:9620` |
| Health | — | `:9621` (`/healthz`) |

## Build / test

```bash
CGO_ENABLED=0 go test ./...   # httptest mocks only; no network credentials
CGO_ENABLED=0 go build -o bin/downloader-sabnzbd ./cmd/module
```

## Status

v0.1.0 — HTTP API client + gRPC service + offline completed-event path. Optional peer (not default host).
