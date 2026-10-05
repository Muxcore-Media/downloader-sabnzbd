# Changelog

## [0.1.1] - 2026-10-05

### Changed
- CI runs on GitHub-hosted runners from the umbrella template; retired-origin workflows removed.
- Dependencies resolve from published GitHub tags (no filesystem `replace`); requires core v0.6.0.

## [v0.1.1] — 2026-08-10

### Added
- Fixture engine (`SABNZBD_FIXTURE=1` / `DOWNLOADER_ENGINE=fixture`) for offline grabs without live SAB
- `nzb_content` on AddNZB + `Client.AddFile` (mode=addfile)
- History file slots in `download.completed` payload (`HistoryFiles`)
- `/health` alongside `/healthz` (503 when configured SAB is down)
- API key via `X-SABnzbd-Apikey` header
- `HistoryJob` lookup (avoids missing jobs outside last-N history)
- `download.failed` on 72h watch deadline
- `httptest` SABnzbd mock (`NewMockServer`) for offline CI
- History poll → `download.started` / `download.completed` / `download.failed` events
- `OfflineDispatch` for automation-style offline NZB → completed path
- Soft-empty when `SABNZBD_URL` / API key unset; credentials documented as operator opt-in
- golangci-lint in CI; settings tests

## [v0.1.0] — 2026-08-10

### Added
- SABnzbd HTTP API client (addurl, queue, pause/resume/delete, history)
- `UsenetDownloaderService` gRPC + SettingsProvider
- Health endpoint `:9621`
