# Changelog

## [v0.1.1] — 2026-08-10

### Added
- `httptest` SABnzbd mock (`NewMockServer`) for offline CI
- History poll → `download.started` / `download.completed` / `download.failed` events
- `OfflineDispatch` for automation-style offline NZB → completed path
- Soft-empty when `SABNZBD_URL` / API key unset; credentials documented as operator opt-in

## [v0.1.0] — 2026-08-10

### Added
- SABnzbd HTTP API client (addurl, queue, pause/resume/delete, history)
- `UsenetDownloaderService` gRPC + SettingsProvider
- Health endpoint `:9621`
