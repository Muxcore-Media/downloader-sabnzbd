package sabnzbd_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Muxcore-Media/downloader-sabnzbd/internal/sabnzbd"
)

func TestClientAddAndQueue(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("apikey") != "secret" {
			http.Error(w, "bad key", 403)
			return
		}
		switch q.Get("mode") {
		case "addurl":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":  true,
				"nzo_ids": []string{"SABnzbd_nzo_abc"},
			})
		case "queue":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"queue": map[string]any{
					"slots": []map[string]any{{
						"nzo_id":     "SABnzbd_nzo_abc",
						"filename":   "Example.nzb",
						"status":     "Downloading",
						"cat":        "movies",
						"percentage": "42.5",
						"mb":         "1000",
						"mbleft":     "575",
					}},
				},
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"status": true})
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := &sabnzbd.Client{BaseURL: srv.URL, APIKey: "secret", HTTPClient: srv.Client()}
	id, err := c.AddURL(context.Background(), "https://example.test/a.nzb", "Example", "movies", false)
	if err != nil {
		t.Fatal(err)
	}
	if id != "SABnzbd_nzo_abc" {
		t.Fatalf("id=%q", id)
	}
	items, err := c.Queue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Name != "Example.nzb" {
		t.Fatalf("queue=%+v", items)
	}
}
