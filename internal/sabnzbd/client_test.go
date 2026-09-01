package sabnzbd_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Muxcore-Media/downloader-sabnzbd/internal/sabnzbd"
)

func TestClientAddQueueHistoryOffline(t *testing.T) {
	mock := sabnzbd.NewMockServer("secret")
	defer mock.Close()

	c := mock.Client()
	id, err := c.AddURL(context.Background(), "https://example.test/a.nzb", "Example", "movies", false)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("empty job id")
	}
	items, err := c.Queue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Name != "Example" {
		t.Fatalf("queue=%+v", items)
	}
	if !mock.Complete(id, "/downloads/Example") {
		t.Fatal("complete failed")
	}
	hist, err := c.History(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Status != "Completed" || hist[0].Storage == "" {
		t.Fatalf("history=%+v", hist)
	}
	job, err := c.HistoryJob(context.Background(), id)
	if err != nil || job == nil || job.ID != id {
		t.Fatalf("history job=%+v err=%v", job, err)
	}
	files, err := c.HistoryFiles(context.Background(), id)
	if err != nil || len(files) < 2 {
		t.Fatalf("files=%+v err=%v", files, err)
	}
}

func TestClientAddFile(t *testing.T) {
	mock := sabnzbd.NewMockServer("secret")
	defer mock.Close()
	c := mock.Client()
	id, err := c.AddFile(context.Background(), []byte(`<?xml version="1.0"?><nzb></nzb>`), "Inline", "movies", true)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("empty job id")
	}
	items, _ := c.Queue(context.Background())
	if len(items) != 1 {
		t.Fatalf("queue=%+v", items)
	}
}

func TestClientAddURLValidation(t *testing.T) {
	mock := sabnzbd.NewMockServer("secret")
	defer mock.Close()
	c := mock.Client()
	for _, url := range []string{"", "ftp://x/a.nzb", "not-a-url", "http://"} {
		_, err := c.AddURL(context.Background(), url, "x", "", false)
		if err == nil {
			t.Fatalf("expected error for %q", url)
		}
	}
}

func TestClientUsesAPIKeyHeader(t *testing.T) {
	mock := sabnzbd.NewMockServer("header-key")
	defer mock.Close()
	c := mock.Client()
	if _, err := c.Queue(context.Background()); err != nil {
		t.Fatal(err)
	}
	cBad := &sabnzbd.Client{BaseURL: mock.URL(), APIKey: "wrong", HTTPClient: mock.Server.Client()}
	_, err := cBad.Queue(context.Background())
	if err == nil || !strings.Contains(err.Error(), "403") && !strings.Contains(err.Error(), "Forbidden") {
		// mock returns 403 body
		if err == nil {
			t.Fatal("expected auth error")
		}
	}
}

func TestClientPauseResumeDelete(t *testing.T) {
	mock := sabnzbd.NewMockServer("k")
	defer mock.Close()
	c := mock.Client()
	id, err := c.AddURL(context.Background(), "https://example.test/b.nzb", "B", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Pause(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	items, _ := c.Queue(context.Background())
	if len(items) != 1 || items[0].Status != "Paused" {
		t.Fatalf("after pause: %+v", items)
	}
	if err := c.Resume(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	items, _ = c.Queue(context.Background())
	if len(items) != 1 || items[0].Status != "Downloading" {
		t.Fatalf("after resume: %+v", items)
	}
	if err := c.Delete(context.Background(), id, false); err != nil {
		t.Fatal(err)
	}
	items, _ = c.Queue(context.Background())
	if len(items) != 0 {
		t.Fatalf("after delete: %+v", items)
	}
}

func TestClientRejectsBadKey(t *testing.T) {
	mock := sabnzbd.NewMockServer("secret")
	defer mock.Close()
	c := &sabnzbd.Client{BaseURL: mock.URL(), APIKey: "wrong", HTTPClient: mock.Server.Client()}
	_, err := c.Queue(context.Background())
	if err == nil {
		t.Fatal("expected auth error")
	}
}

func TestFixtureClientCompletes(t *testing.T) {
	f := sabnzbd.NewFixtureClient()
	id, err := f.AddURL(context.Background(), "https://example.test/a.nzb", "Example", "movies", false)
	if err != nil {
		t.Fatal(err)
	}
	h, err := f.HistoryJob(context.Background(), id)
	if err != nil || h == nil || h.Status != "Completed" {
		t.Fatalf("history=%+v err=%v", h, err)
	}
	files, err := f.HistoryFiles(context.Background(), id)
	if err != nil || len(files) < 2 {
		t.Fatalf("files=%+v", files)
	}
}
