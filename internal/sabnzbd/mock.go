package sabnzbd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
)

// MockServer is an in-process SABnzbd /api stand-in for offline tests.
// No network credentials required.
type MockServer struct {
	APIKey string
	Server *httptest.Server

	mu      sync.Mutex
	seq     atomic.Uint64
	queue   map[string]*mockJob
	history map[string]*mockJob
}

type mockJob struct {
	ID       string
	Name     string
	Category string
	Status   string
	Storage  string
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

func (m *MockServer) handleAPI(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("apikey") != m.APIKey {
		http.Error(w, `{"error":"API Key Incorrect"}`, http.StatusForbidden)
		return
	}
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
