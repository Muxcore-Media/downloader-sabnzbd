package sabnzbd

import "context"

// API is the SABnzbd backend (live HTTP client or in-memory fixture).
type API interface {
	AddURL(ctx context.Context, nzbURL, name, category string, paused bool) (string, error)
	AddFile(ctx context.Context, content []byte, name, category string, paused bool) (string, error)
	Queue(ctx context.Context) ([]QueueItem, error)
	Pause(ctx context.Context, id string) error
	Resume(ctx context.Context, id string) error
	Delete(ctx context.Context, id string, deleteFiles bool) error
	History(ctx context.Context, limit int) ([]HistoryItem, error)
	HistoryJob(ctx context.Context, id string) (*HistoryItem, error)
	HistoryFiles(ctx context.Context, id string) ([]HistoryFile, error)
}

// HistoryFile is one extracted file from a completed download.
type HistoryFile struct {
	Path string
	Size int64
}
