// Package sabnzbd implements a minimal SABnzbd HTTP API client.
package sabnzbd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const apiKeyHeader = "X-SABnzbd-Apikey"

// Client talks to SABnzbd's /api endpoint.
type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

func (c *Client) http() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func validateNZBURL(nzbURL string) error {
	if strings.TrimSpace(nzbURL) == "" {
		return fmt.Errorf("nzb_url required")
	}
	u, err := url.Parse(nzbURL)
	if err != nil {
		return fmt.Errorf("nzb_url must be http(s): %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("nzb_url must be http(s)")
	}
	if u.Host == "" {
		return fmt.Errorf("nzb_url must be http(s)")
	}
	return nil
}

func parseJobID(out map[string]any, mode string) (string, error) {
	ids, ok := out["nzo_ids"].([]any)
	if !ok || len(ids) == 0 {
		return "", fmt.Errorf("sabnzbd %s: empty nzo_ids in response", mode)
	}
	id := strings.TrimSpace(fmt.Sprint(ids[0]))
	if id == "" {
		return "", fmt.Errorf("sabnzbd %s: empty nzo_ids in response", mode)
	}
	return id, nil
}

func (c *Client) call(ctx context.Context, mode string, extra url.Values) (map[string]any, error) {
	return c.callWithBody(ctx, http.MethodGet, mode, extra, nil, "")
}

func (c *Client) callWithBody(ctx context.Context, method, mode string, extra url.Values, body io.Reader, contentType string) (map[string]any, error) {
	if c.BaseURL == "" {
		return nil, fmt.Errorf("sabnzbd base URL required")
	}
	if c.APIKey == "" {
		return nil, fmt.Errorf("sabnzbd API key required")
	}
	u, err := url.Parse(strings.TrimRight(c.BaseURL, "/") + "/api")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("mode", mode)
	q.Set("output", "json")
	for k, vs := range extra {
		for _, v := range vs {
			q.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(apiKeyHeader, c.APIKey)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sabnzbd %s: HTTP %d: %s", mode, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("sabnzbd %s decode: %w", mode, err)
	}
	if st, _ := out["status"].(bool); !st && out["error"] != nil {
		return nil, fmt.Errorf("sabnzbd %s: %v", mode, out["error"])
	}
	return out, nil
}

// AddURL queues an NZB by URL.
func (c *Client) AddURL(ctx context.Context, nzbURL, name, category string, paused bool) (jobID string, err error) {
	if err := validateNZBURL(nzbURL); err != nil {
		return "", err
	}
	extra := url.Values{}
	extra.Set("name", nzbURL)
	if name != "" {
		extra.Set("nzbname", name)
	}
	if category != "" {
		extra.Set("cat", category)
	}
	if paused {
		extra.Set("priority", "-2") // paused
	}
	out, err := c.call(ctx, "addurl", extra)
	if err != nil {
		return "", err
	}
	return parseJobID(out, "addurl")
}

// AddFile queues NZB XML bytes (mode=addfile).
func (c *Client) AddFile(ctx context.Context, content []byte, name, category string, paused bool) (string, error) {
	if len(content) == 0 {
		return "", fmt.Errorf("nzb_content required")
	}
	extra := url.Values{}
	if name != "" {
		extra.Set("nzbname", name)
	}
	if category != "" {
		extra.Set("cat", category)
	}
	if paused {
		extra.Set("priority", "-2")
	}
	out, err := c.callWithBody(ctx, http.MethodPost, "addfile", extra, bytes.NewReader(content), "application/x-nzb")
	if err != nil {
		return "", err
	}
	return parseJobID(out, "addfile")
}

// QueueItem is a simplified queue row.
type QueueItem struct {
	ID         string
	Name       string
	Status     string
	Category   string
	Percentage float64
	SizeMB     float64
	Remaining  float64
}

// Queue returns current download slots.
func (c *Client) Queue(ctx context.Context) ([]QueueItem, error) {
	out, err := c.call(ctx, "queue", nil)
	if err != nil {
		return nil, err
	}
	q, _ := out["queue"].(map[string]any)
	if q == nil {
		return nil, nil
	}
	slots, _ := q["slots"].([]any)
	items := make([]QueueItem, 0, len(slots))
	for _, s := range slots {
		m, _ := s.(map[string]any)
		if m == nil {
			continue
		}
		pct, _ := strconv.ParseFloat(fmt.Sprint(m["percentage"]), 64)
		mb, _ := strconv.ParseFloat(strings.TrimSuffix(fmt.Sprint(m["mb"]), " "), 64)
		mbleft, _ := strconv.ParseFloat(strings.TrimSuffix(fmt.Sprint(m["mbleft"]), " "), 64)
		items = append(items, QueueItem{
			ID:         fmt.Sprint(m["nzo_id"]),
			Name:       fmt.Sprint(m["filename"]),
			Status:     fmt.Sprint(m["status"]),
			Category:   fmt.Sprint(m["cat"]),
			Percentage: pct,
			SizeMB:     mb,
			Remaining:  mbleft,
		})
	}
	return items, nil
}

// Pause pauses one job or the whole queue when id is empty.
func (c *Client) Pause(ctx context.Context, id string) error {
	if id == "" {
		_, err := c.call(ctx, "pause", nil)
		return err
	}
	_, err := c.call(ctx, "queue", url.Values{"name": {"pause"}, "value": {id}})
	return err
}

// Resume resumes one job or the whole queue when id is empty.
func (c *Client) Resume(ctx context.Context, id string) error {
	if id == "" {
		_, err := c.call(ctx, "resume", nil)
		return err
	}
	_, err := c.call(ctx, "queue", url.Values{"name": {"resume"}, "value": {id}})
	return err
}

// Delete removes a queue item.
func (c *Client) Delete(ctx context.Context, id string, deleteFiles bool) error {
	extra := url.Values{"name": {"delete"}, "value": {id}}
	if deleteFiles {
		extra.Set("del_files", "1")
	}
	_, err := c.call(ctx, "queue", extra)
	return err
}

// HistoryItem is a simplified history row.
type HistoryItem struct {
	ID       string
	Name     string
	Status   string
	Category string
	Storage  string
}

// History returns completed downloads.
func (c *Client) History(ctx context.Context, limit int) ([]HistoryItem, error) {
	extra := url.Values{}
	if limit > 0 {
		extra.Set("limit", strconv.Itoa(limit))
	}
	out, err := c.call(ctx, "history", extra)
	if err != nil {
		return nil, err
	}
	return parseHistorySlots(out)
}

// HistoryJob returns one history row by NZO id (avoids missing jobs outside the last N slots).
func (c *Client) HistoryJob(ctx context.Context, id string) (*HistoryItem, error) {
	if id == "" {
		return nil, nil
	}
	out, err := c.call(ctx, "history", url.Values{"name": {"show"}, "value": {id}})
	if err != nil {
		return nil, err
	}
	items, err := parseHistorySlots(out)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		if it.ID == id {
			cp := it
			return &cp, nil
		}
	}
	if len(items) == 1 {
		cp := items[0]
		return &cp, nil
	}
	return nil, nil
}

// HistoryFiles returns extracted file paths for a completed download.
func (c *Client) HistoryFiles(ctx context.Context, id string) ([]HistoryFile, error) {
	if id == "" {
		return nil, nil
	}
	out, err := c.call(ctx, "history", url.Values{"name": {"files"}, "value": {id}})
	if err != nil {
		return nil, err
	}
	return parseHistoryFiles(out)
}

func parseHistorySlots(out map[string]any) ([]HistoryItem, error) {
	h, _ := out["history"].(map[string]any)
	if h == nil {
		return nil, nil
	}
	slots, _ := h["slots"].([]any)
	items := make([]HistoryItem, 0, len(slots))
	for _, s := range slots {
		m, _ := s.(map[string]any)
		if m == nil {
			continue
		}
		items = append(items, HistoryItem{
			ID:       fmt.Sprint(m["nzo_id"]),
			Name:     fmt.Sprint(m["name"]),
			Status:   fmt.Sprint(m["status"]),
			Category: fmt.Sprint(m["category"]),
			Storage:  fmt.Sprint(m["storage"]),
		})
	}
	return items, nil
}

func parseHistoryFiles(resp map[string]any) ([]HistoryFile, error) {
	h, _ := resp["history"].(map[string]any)
	if h == nil {
		return nil, nil
	}
	// files mode may return a flat "files" list or slot-embedded files.
	if files, ok := h["files"].([]any); ok {
		return mapFileEntries(files), nil
	}
	slots, _ := h["slots"].([]any)
	var out []HistoryFile
	for _, s := range slots {
		m, _ := s.(map[string]any)
		if m == nil {
			continue
		}
		if files, ok := m["files"].([]any); ok {
			out = append(out, mapFileEntries(files)...)
		}
	}
	return out, nil
}

func mapFileEntries(files []any) []HistoryFile {
	out := make([]HistoryFile, 0, len(files))
	for _, f := range files {
		m, _ := f.(map[string]any)
		if m == nil {
			continue
		}
		path := fmt.Sprint(m["filename"])
		if path == "" {
			path = fmt.Sprint(m["path"])
		}
		size, _ := strconv.ParseInt(fmt.Sprint(m["size"]), 10, 64)
		if size == 0 {
			size, _ = strconv.ParseInt(fmt.Sprint(m["bytes"]), 10, 64)
		}
		out = append(out, HistoryFile{Path: path, Size: size})
	}
	return out
}
