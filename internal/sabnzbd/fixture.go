package sabnzbd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// FixtureClient is an in-memory SABnzbd stand-in for CI / DOWNLOADER_ENGINE=fixture.
// No HTTP and no live SABnzbd required.
type FixtureClient struct {
	queue   map[string]*mockJob
	history map[string]*mockJob
	seq     atomic.Uint64
	mu      sync.Mutex
}

// NewFixtureClient returns an empty fixture store.
func NewFixtureClient() *FixtureClient {
	return &FixtureClient{
		queue:   map[string]*mockJob{},
		history: map[string]*mockJob{},
	}
}

func (f *FixtureClient) nextID() string {
	return fmt.Sprintf("SABnzbd_nzo_%d", f.seq.Add(1))
}

func (f *FixtureClient) addJob(name, category string, paused bool) (string, *mockJob) {
	if name == "" {
		name = "fixture"
	}
	id := f.nextID()
	job := &mockJob{
		ID: id, Name: name, Category: category,
		Status: "Downloading", Pct: 10, SizeMB: 100, LeftMB: 90,
		Storage: "/downloads/" + name,
		Files:   defaultFixtureFiles("/downloads/"+name, name),
	}
	if paused {
		job.Status = "Paused"
	} else {
		f.completeLocked(job)
	}
	f.queue[id] = job
	return id, job
}

func defaultFixtureFiles(storage, name string) []HistoryFile {
	return []HistoryFile{
		{Path: storage + "/" + name + ".mkv", Size: 8192},
		{Path: storage + "/" + name + ".srt", Size: 512},
	}
}

func (f *FixtureClient) completeLocked(j *mockJob) {
	delete(f.queue, j.ID)
	j.Status = "Completed"
	j.Pct = 100
	j.LeftMB = 0
	if j.Storage == "" {
		j.Storage = "/downloads/" + j.Name
	}
	if len(j.Files) == 0 {
		j.Files = defaultFixtureFiles(j.Storage, j.Name)
	}
	f.history[j.ID] = j
}

func (f *FixtureClient) AddURL(_ context.Context, nzbURL, name, category string, paused bool) (string, error) {
	if err := validateNZBURLSyntax(nzbURL); err != nil {
		return "", err
	}
	if name == "" {
		name = nzbURL
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id, job := f.addJob(name, category, paused)
	if !paused {
		f.completeLocked(job)
	}
	return id, nil
}

func (f *FixtureClient) AddFile(_ context.Context, content []byte, name, category string, paused bool) (string, error) {
	if len(content) == 0 {
		return "", fmt.Errorf("nzb_content required")
	}
	if name == "" {
		name = "fixture.nzb"
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id, job := f.addJob(name, category, paused)
	_ = content
	if !paused {
		f.completeLocked(job)
	}
	return id, nil
}

func (f *FixtureClient) Queue(_ context.Context) ([]QueueItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]QueueItem, 0, len(f.queue))
	for _, j := range f.queue {
		items = append(items, jobToQueueItem(j))
	}
	return items, nil
}

func jobToQueueItem(j *mockJob) QueueItem {
	return QueueItem{
		ID: j.ID, Name: j.Name, Status: j.Status, Category: j.Category,
		Percentage: j.Pct, SizeMB: j.SizeMB, Remaining: j.LeftMB,
	}
}

func (f *FixtureClient) Pause(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id == "" {
		for _, j := range f.queue {
			j.Status = "Paused"
		}
		return nil
	}
	j, ok := f.queue[id]
	if !ok {
		return fmt.Errorf("job %q not found", id)
	}
	j.Status = "Paused"
	return nil
}

func (f *FixtureClient) Resume(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id == "" {
		for _, j := range f.queue {
			j.Status = "Downloading"
			f.completeLocked(j)
		}
		return nil
	}
	j, ok := f.queue[id]
	if !ok {
		return fmt.Errorf("job %q not found", id)
	}
	j.Status = "Downloading"
	f.completeLocked(j)
	return nil
}

func (f *FixtureClient) Delete(_ context.Context, id string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.queue[id]; !ok {
		return fmt.Errorf("job %q not found", id)
	}
	delete(f.queue, id)
	return nil
}

func (f *FixtureClient) History(_ context.Context, limit int) ([]HistoryItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]HistoryItem, 0, len(f.history))
	for _, j := range f.history {
		items = append(items, jobToHistoryItem(j))
	}
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (f *FixtureClient) HistoryJob(_ context.Context, id string) (*HistoryItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.history[id]
	if !ok {
		return nil, nil
	}
	item := jobToHistoryItem(j)
	return &item, nil
}

func (f *FixtureClient) HistoryFiles(_ context.Context, id string) ([]HistoryFile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.history[id]
	if !ok {
		return nil, nil
	}
	out := make([]HistoryFile, len(j.Files))
	copy(out, j.Files)
	return out, nil
}

func jobToHistoryItem(j *mockJob) HistoryItem {
	return HistoryItem{
		ID: j.ID, Name: j.Name, Status: j.Status, Category: j.Category, Storage: j.Storage,
	}
}

// Fail moves a queue job into history as Failed (test helper).
func (f *FixtureClient) Fail(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.queue[id]
	if !ok {
		return false
	}
	delete(f.queue, id)
	j.Status = "Failed"
	f.history[id] = j
	return true
}
