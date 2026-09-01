# Downloader SABnzbd

MuxCore sidecar that bridges [SABnzbd](https://sabnzbd.org/) for NZB/usenet downloads.

Exposes `muxcore.usenet.v1.UsenetDownloaderService` (AddNZB, queue, pause/resume/delete, history) plus SettingsProvider.

## Network safety / CI

- **Unit tests and CI never talk to a real SABnzbd or usenet provider.** Use `DOWNLOADER_ENGINE=fixture` / `SABNZBD_FIXTURE=1` (in-memory) or the httptest mock (`internal/sabnzbd.NewMockServer`).
- Live SABnzbd is **operator opt-in only**: set `SABNZBD_URL` + `SABNZBD_API_KEY` (or admin settings). Leave them empty for soft-empty health (optional peer).
- Product gates and laptop demos do **not** require Usenet credentials.

## Offline / mock path

| Mode | When |
|------|------|
| `DOWNLOADER_ENGINE=fixture` (or `fake`) | In-memory backend; AddNZB/OfflineDispatch complete without live SAB |
| `SABNZBD_FIXTURE=1` | Same as fixture engine |
| httptest mock | Unit tests with `NewMockServer` |

When a publisher is injected (tests or mesh adapter), events are emitted:

| Event | When |
|-------|------|
| `download.started` | NZB accepted |
| `download.completed` | History status contains `Complete` (payload `files` from SAB history slots) |
| `download.failed` | History status contains `Fail`, or 72h watch deadline |

`AddNZB` accepts `nzb_url` (HTTP addurl) or `nzb_content` bytes (addfile POST). Content is preferred when both are set.

## Config

| Env | Setting | Default |
|-----|---------|---------|
| `SABNZBD_URL` | `base_url` | — (empty = unconfigured / soft-empty) |
| `SABNZBD_API_KEY` | `api_key` | — (operator opt-in) |
| `SABNZBD_FIXTURE` / `DOWNLOADER_ENGINE` | fixture mode | off |
| gRPC listen | — | `:9620` |
| Health | — | `:9621` (`/health` and `/healthz`) |

API key is sent as `X-SABnzbd-Apikey` (not query string).

## Build / test

```bash
CGO_ENABLED=0 go test ./...   # fixture + httptest only; no network credentials
CGO_ENABLED=0 go build -o bin/downloader-sabnzbd ./cmd/module
```

## Status

v0.1.1 — HTTP API client + gRPC service + fixture/offline completed-event path. Optional peer (not default host).
