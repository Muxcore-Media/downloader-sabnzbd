# Compatibility

## Core Version

| Module Version | Core Version | Status |
|----------------|-------------|--------|
| v0.1.1         | 0.5.8+      | Current |

## Capabilities

- `downloader` / `downloader.usenet` / `usenet`
- `settings`

## Notes

Talks to an external SABnzbd instance via HTTP API (or in-memory fixture). Does not embed a usenet engine.
API key is sent via `X-SABnzbd-Apikey` header.
