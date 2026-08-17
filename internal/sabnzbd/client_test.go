package sabnzbd_test

import (
	"context"
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
	if !mock.Complete(id, "/downloads/Example/Example.mkv") {
		t.Fatal("complete failed")
	}
	hist, err := c.History(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Status != "Completed" || hist[0].Storage == "" {
		t.Fatalf("history=%+v", hist)
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
