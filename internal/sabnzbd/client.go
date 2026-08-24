// Package sabnzbd implements a minimal SABnzbd HTTP API client.
package sabnzbd

import (
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

func (c *Client) call(ctx context.Context, mode string, extra url.Values) (map[string]any, error) {
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
	q.Set("apikey", c.APIKey)
	q.Set("output", "json")
	for k, vs := range extra {
		for _, v := range vs {
			q.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sabnzbd %s: HTTP %d: %s", mode, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("sabnzbd %s decode: %w", mode, err)
	}
	if st, _ := out["status"].(bool); !st && out["error"] != nil {
		return nil, fmt.Errorf("sabnzbd %s: %v", mode, out["error"])
	}
	return out, nil
}

// AddURL queues an NZB by URL.
func (c *Client) AddURL(ctx context.Context, nzbURL, name, category string, paused bool) (jobID string, err error) {
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
	if ids, ok := out["nzo_ids"].([]any); ok && len(ids) > 0 {
		return fmt.Sprint(ids[0]), nil
	}
	return "", nil
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
