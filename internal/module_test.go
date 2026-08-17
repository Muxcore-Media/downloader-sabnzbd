package internal_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/Muxcore-Media/downloader-sabnzbd/internal"
	"github.com/Muxcore-Media/downloader-sabnzbd/internal/sabnzbd"
)

type recPub struct {
	mu   sync.Mutex
	evts []recEvt
}

type recEvt struct {
	Type    string
	Payload contracts.DownloadEventPayload
}

func (r *recPub) Publish(_ context.Context, eventType string, payload []byte) error {
	var p contracts.DownloadEventPayload
	_ = json.Unmarshal(payload, &p)
	r.mu.Lock()
	r.evts = append(r.evts, recEvt{Type: eventType, Payload: p})
	r.mu.Unlock()
	return nil
}

func (r *recPub) wait(t *testing.T, typ string, d time.Duration) recEvt {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for _, e := range r.evts {
			if e.Type == typ {
				r.mu.Unlock()
				return e
			}
		}
		r.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s; got %+v", typ, r.snapshot())
	return recEvt{}
}

func (r *recPub) snapshot() []recEvt {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recEvt, len(r.evts))
	copy(out, r.evts)
	return out
}

func TestOfflineDispatchCompletedEvent(t *testing.T) {
	mock := sabnzbd.NewMockServer("ci-key")
	defer mock.Close()

	pub := &recPub{}
	m := internal.NewModule(internal.Config{
		BaseURL:    mock.URL(),
		APIKey:     "ci-key",
		GRPCAddr:   "127.0.0.1:0",
		HTTPAddr:   "127.0.0.1:0",
		Publish:    pub.Publish,
		PollEvery:  10 * time.Millisecond,
		HTTPClient: mock.Server.Client(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var jobID string
	var dispatchErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		jobID, dispatchErr = m.OfflineDispatch(ctx, "https://fixture.test/movie.nzb", "Fixture.Movie", "movies")
	}()

	// Wait until job is queued, then complete it on the mock SAB.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		items, err := mock.Client().Queue(context.Background())
		if err == nil && len(items) == 1 {
			if !mock.Complete(items[0].ID, "/downloads/Fixture.Movie/Fixture.Movie.mkv") {
				t.Fatal("mock complete failed")
			}
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	<-done
	if dispatchErr != nil {
		t.Fatal(dispatchErr)
	}
	if jobID == "" {
		t.Fatal("empty job id")
	}

	started := pub.wait(t, contracts.EventDownloadStarted, time.Second)
	if started.Payload.ID != jobID {
		t.Fatalf("started id=%q want %q", started.Payload.ID, jobID)
	}
	completed := pub.wait(t, contracts.EventDownloadCompleted, time.Second)
	if completed.Payload.SavePath == "" {
		t.Fatalf("completed missing save_path: %+v", completed.Payload)
	}
	if completed.Payload.Label != "usenet" {
		t.Fatalf("label=%q", completed.Payload.Label)
	}
}

func TestUnconfiguredSoftEmpty(t *testing.T) {
	m := internal.NewModule(internal.Config{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Health(context.Background()); err != nil {
		t.Fatalf("unconfigured health should be soft-ok: %v", err)
	}
	_, err := m.OfflineDispatch(context.Background(), "https://x/a.nzb", "x", "")
	if err == nil {
		t.Fatal("expected unconfigured error")
	}
}
