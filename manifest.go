package manifest

import _ "embed"

// ManifestJSON is the module's muxcore.json, embedded so the reported module
// version has a single source (ADR-0021).
//
//go:embed muxcore.json
var ManifestJSON []byte
