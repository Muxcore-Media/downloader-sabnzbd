package sabnzbd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
)

// MockServer is an in-process SABnzbd /api stand-in for offline tests.
// No network credentials required.
type MockServer struct {
	Server  *httptest.Server
	queue   map[string]*mockJob
	history map[string]*mockJob
	APIKey  string
	seq     atomic.Uint64
	mu      sync.Mutex
}

type mockJob struct {
	ID       string
	Name     string
	Category string
	Status   string
	Storage  string
	Files    []HistoryFile
	Pct      float64
	SizeMB   float64
	LeftMB   float64
}

// NewMockServer starts an httptest SABnzbd API that accepts the given API key.
func NewMockServer(apiKey string) *MockServer {
	if apiKey == "" {
		apiKey = "test-key"
	}
	m := &MockServer{
		APIKey:  apiKey,
		queue:   map[string]*mockJob{},
		history: map[string]*mockJob{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api", m.handleAPI)
	m.Server = httptest.NewServer(mux)
	return m
}

// URL is the SABnzbd base URL (without /api).
func (m *MockServer) URL() string { return m.Server.URL }

// Close shuts down the httptest server.
func (m *MockServer) Close() { m.Server.Close() }

// Client returns a sabnzbd.Client pointed at this mock.
func (m *MockServer) Client() *Client {
	return &Client{
		BaseURL:    m.URL(),
		APIKey:     m.APIKey,
		HTTPClient: m.Server.Client(),
	}
}

// Complete moves a queue job into history as Completed (download finished).
func (m *MockServer) Complete(id, storage string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.queue[id]
	if !ok {
		return false
	}
	delete(m.queue, id)
	j.Status = "Completed"
	j.Pct = 100
	j.LeftMB = 0
	if storage != "" {
		j.Storage = storage
	} else if j.Storage == "" {
		j.Storage = "/downloads/" + j.Name
	}
	if len(j.Files) == 0 {
		j.Files = defaultFixtureFiles(j.Storage, j.Name)
	}
	m.history[id] = j
	return true
}

// Fail moves a queue job into history as Failed.
func (m *MockServer) Fail(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.queue[id]
	if !ok {
		return false
	}
	delete(m.queue, id)
	j.Status = "Failed"
	m.history[id] = j
	return true
}

func (m *MockServer) authOK(r *http.Request) bool {
	key := r.Header.Get(apiKeyHeader)
	if key == "" {
		key = r.URL.Query().Get("apikey") // legacy SAB / proxy fallback
	}
	return key == m.APIKey
}

func (m *MockServer) handleAPI(w http.ResponseWriter, r *http.Request) {
	if !m.authOK(r) {
		http.Error(w, `{"error":"API Key Incorrect"}`, http.StatusForbidden)
		return
	}
	q := r.URL.Query()
	mode := q.Get("mode")
	switch mode {
	case "addurl":
		m.mu.Lock()
		id := fmt.Sprintf("SABnzbd_nzo_%d", m.seq.Add(1))
		name := q.Get("nzbname")
		if name == "" {
			name = q.Get("name")
		}
		job := &mockJob{
			ID: id, Name: name, Category: q.Get("cat"),
			Status: "Downloading", Pct: 10, SizeMB: 100, LeftMB: 90,
			Storage: "/downloads/" + name,
		}
		if q.Get("priority") == "-2" {
			job.Status = "Paused"
		}
		m.queue[id] = job
		m.mu.Unlock()
		writeJSON(w, map[string]any{"status": true, "nzo_ids": []string{id}})
	case "addfile":
		name := q.Get("nzbname")
		if name == "" {
			name = "upload.nzb"
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if len(body) == 0 {
			http.Error(w, `{"error":"empty nzb"}`, http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		id := fmt.Sprintf("SABnzbd_nzo_%d", m.seq.Add(1))
		job := &mockJob{
			ID: id, Name: name, Category: q.Get("cat"),
			Status: "Downloading", Pct: 10, SizeMB: 100, LeftMB: 90,
			Storage: "/downloads/" + name,
		}
		if q.Get("priority") == "-2" {
			job.Status = "Paused"
		}
		m.queue[id] = job
		m.mu.Unlock()
		writeJSON(w, map[string]any{"status": true, "nzo_ids": []string{id}})
	case "queue":
		name := q.Get("name")
		value := q.Get("value")
		if name == "pause" || name == "resume" || name == "delete" {
			m.mu.Lock()
			if j, ok := m.queue[value]; ok {
				switch name {
				case "pause":
					j.Status = "Paused"
				case "resume":
					j.Status = "Downloading"
				case "delete":
					delete(m.queue, value)
				}
			}
			m.mu.Unlock()
			writeJSON(w, map[string]any{"status": true})
			return
		}
		m.mu.Lock()
		slots := make([]map[string]any, 0, len(m.queue))
		for _, j := range m.queue {
			slots = append(slots, map[string]any{
				"nzo_id": j.ID, "filename": j.Name, "status": j.Status,
				"cat": j.Category, "percentage": fmt.Sprintf("%.1f", j.Pct),
				"mb": fmt.Sprintf("%.0f", j.SizeMB), "mbleft": fmt.Sprintf("%.0f", j.LeftMB),
			})
		}
		m.mu.Unlock()
		writeJSON(w, map[string]any{"queue": map[string]any{"slots": slots}})
	case "pause":
		m.mu.Lock()
		for _, j := range m.queue {
			j.Status = "Paused"
		}
		m.mu.Unlock()
		writeJSON(w, map[string]any{"status": true})
	case "resume":
		m.mu.Lock()
		for _, j := range m.queue {
			j.Status = "Downloading"
		}
		m.mu.Unlock()
		writeJSON(w, map[string]any{"status": true})
	case "history":
		switch q.Get("name") {
		case "show", "files":
			id := q.Get("value")
			m.mu.Lock()
			j, ok := m.history[id]
			m.mu.Unlock()
			if !ok {
				writeJSON(w, map[string]any{"history": map[string]any{"slots": []any{}}})
				return
			}
			slot := map[string]any{
				"nzo_id": j.ID, "name": j.Name, "status": j.Status,
				"category": j.Category, "storage": j.Storage,
			}
			if q.Get("name") == "files" {
				files := make([]map[string]any, 0, len(j.Files))
				for _, f := range j.Files {
					files = append(files, map[string]any{"filename": f.Path, "size": strconv.FormatInt(f.Size, 10)})
				}
				writeJSON(w, map[string]any{"history": map[string]any{"files": files}})
				return
			}
			writeJSON(w, map[string]any{"history": map[string]any{"slots": []any{slot}}})
			return
		}
		m.mu.Lock()
		slots := make([]map[string]any, 0, len(m.history))
		for _, j := range m.history {
			slots = append(slots, map[string]any{
				"nzo_id": j.ID, "name": j.Name, "status": j.Status,
				"category": j.Category, "storage": j.Storage,
			})
		}
		m.mu.Unlock()
		writeJSON(w, map[string]any{"history": map[string]any{"slots": slots}})
	default:
		writeJSON(w, map[string]any{"status": true})
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
