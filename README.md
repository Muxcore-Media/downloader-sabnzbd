# Downloader SABnzbd

MuxCore sidecar that bridges [SABnzbd](https://sabnzbd.org/) for NZB/usenet downloads.

Exposes `muxcore.usenet.v1.UsenetDownloaderService` (AddNZB, queue, pause/resume/delete, history) plus SettingsProvider.

## Config

| Env | Setting | Default |
|-----|---------|---------|
| `SABNZBD_URL` | `base_url` | — |
| `SABNZBD_API_KEY` | `api_key` | — |
| gRPC listen | — | `:9620` |
| Health | — | `:9621` (`/healthz`) |

## Build / test

```bash
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go build -o bin/downloader-sabnzbd ./cmd/module
```

## Status

v0.1.0 scaffold — HTTP API client + gRPC service. Optional peer (not default host). Automation NZB dispatch wiring is a follow-up.
