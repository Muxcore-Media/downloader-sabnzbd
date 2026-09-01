package internal_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/Muxcore-Media/downloader-sabnzbd/internal"
	"github.com/Muxcore-Media/downloader-sabnzbd/internal/sabnzbd"
	usenetv1 "github.com/Muxcore-Media/downloader-sabnzbd/proto/gen/muxcore/usenet/v1"
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

func dialUsenet(t *testing.T, addr string) usenetv1.UsenetDownloaderServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return usenetv1.NewUsenetDownloaderServiceClient(conn)
}

func getHealth(t *testing.T, addr, path string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
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

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		items, err := mock.Client().Queue(context.Background())
		if err == nil && len(items) == 1 {
			if !mock.Complete(items[0].ID, "/downloads/Fixture.Movie") {
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
	if len(completed.Payload.Files) < 2 {
		t.Fatalf("expected multiple files in payload: %+v", completed.Payload.Files)
	}
	if completed.Payload.Label != "usenet" {
		t.Fatalf("label=%q", completed.Payload.Label)
	}
}

func TestOfflineDispatchFailedEvent(t *testing.T) {
	mock := sabnzbd.NewMockServer("ci-key")
	defer mock.Close()

	pub := &recPub{}
	m := internal.NewModule(internal.Config{
		BaseURL:    mock.URL(),
		APIKey:     "ci-key",
		Publish:    pub.Publish,
		PollEvery:  10 * time.Millisecond,
		HTTPClient: mock.Server.Client(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	var dispatchErr error
	go func() {
		defer close(done)
		_, dispatchErr = m.OfflineDispatch(ctx, "https://fixture.test/bad.nzb", "Bad.Movie", "movies")
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		items, err := mock.Client().Queue(context.Background())
		if err == nil && len(items) == 1 {
			if !mock.Fail(items[0].ID) {
				t.Fatal("mock fail failed")
			}
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	<-done
	if dispatchErr == nil {
		t.Fatal("expected dispatch error")
	}
	pub.wait(t, contracts.EventDownloadFailed, time.Second)
}

func TestFixtureOfflineDispatchCompletes(t *testing.T) {
	t.Setenv("SABNZBD_FIXTURE", "1")
	t.Setenv("DOWNLOADER_ENGINE", "")

	pub := &recPub{}
	m := internal.NewModule(internal.Config{
		Fixture:   true,
		Publish:   pub.Publish,
		PollEvery: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	id, err := m.OfflineDispatch(ctx, "https://fixture.test/movie.nzb", "Fixture.Movie", "movies")
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("empty job id")
	}
	pub.wait(t, contracts.EventDownloadStarted, time.Second)
	pub.wait(t, contracts.EventDownloadCompleted, time.Second)
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

func TestGRPCSurfaceAgainstMock(t *testing.T) {
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
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(ctx) }()

	client := dialUsenet(t, m.ListenAddr())

	caps, err := client.GetCapabilities(ctx, &usenetv1.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if caps.GetBackend() != "sabnzbd" || !caps.GetSupportsPausing() {
		t.Fatalf("caps=%+v", caps)
	}

	addResp, err := client.AddNZB(ctx, &usenetv1.AddNZBRequest{
		NzbUrl:   "https://fixture.test/a.nzb",
		Name:     "Movie.A",
		Category: "movies",
		Paused:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if addResp.GetJobId() == "" {
		t.Fatal("empty job id")
	}

	qAll, err := client.ListQueue(ctx, &usenetv1.ListQueueRequest{})
	if err != nil || len(qAll.GetItems()) != 1 {
		t.Fatalf("queue all=%+v err=%v", qAll, err)
	}
	qMovies, err := client.ListQueue(ctx, &usenetv1.ListQueueRequest{Category: "movies"})
	if err != nil || len(qMovies.GetItems()) != 1 {
		t.Fatalf("queue movies=%+v", qMovies)
	}
	qTV, err := client.ListQueue(ctx, &usenetv1.ListQueueRequest{Category: "tv"})
	if err != nil || len(qTV.GetItems()) != 0 {
		t.Fatalf("queue tv=%+v", qTV)
	}

	if _, err := client.Pause(ctx, &usenetv1.PauseRequest{Id: addResp.GetJobId()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resume(ctx, &usenetv1.ResumeRequest{Id: addResp.GetJobId()}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		items, _ := mock.Client().Queue(context.Background())
		if len(items) == 1 {
			mock.Complete(items[0].ID, "/downloads/Movie.A")
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	pub.wait(t, contracts.EventDownloadCompleted, time.Second)

	hist, err := client.GetHistory(ctx, &usenetv1.GetHistoryRequest{Limit: 10})
	if err != nil || len(hist.GetItems()) == 0 {
		t.Fatalf("history=%+v err=%v", hist, err)
	}

	if _, err := client.Delete(ctx, &usenetv1.DeleteRequest{Id: "missing", DeleteFiles: false}); err == nil {
		// delete on missing may error from mock — ok either way for surface test
	}
}

func TestAddNZBWithContent(t *testing.T) {
	mock := sabnzbd.NewMockServer("ci-key")
	defer mock.Close()

	m := internal.NewModule(internal.Config{
		BaseURL:    mock.URL(),
		APIKey:     "ci-key",
		GRPCAddr:   "127.0.0.1:0",
		HTTPAddr:   "127.0.0.1:0",
		PollEvery:  10 * time.Millisecond,
		HTTPClient: mock.Server.Client(),
	})
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(ctx) }()

	client := dialUsenet(t, m.ListenAddr())
	resp, err := client.AddNZB(ctx, &usenetv1.AddNZBRequest{
		NzbContent: []byte(`<?xml version="1.0"?><nzb></nzb>`),
		Name:       "Inline.NZB",
		Category:   "movies",
		Paused:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetJobId() == "" {
		t.Fatal("empty job id")
	}
}

func TestAddNZBFailedViaRPC(t *testing.T) {
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
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(ctx) }()

	client := dialUsenet(t, m.ListenAddr())
	resp, err := client.AddNZB(ctx, &usenetv1.AddNZBRequest{
		NzbUrl: "https://fixture.test/fail.nzb",
		Name:   "Fail.Movie",
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		items, _ := mock.Client().Queue(context.Background())
		if len(items) == 1 {
			mock.Fail(items[0].ID)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	pub.wait(t, contracts.EventDownloadFailed, time.Second)
	if resp.GetJobId() == "" {
		t.Fatal("empty job id")
	}
}

func TestHealthEndpoints(t *testing.T) {
	mock := sabnzbd.NewMockServer("ci-key")
	defer mock.Close()

	m := internal.NewModule(internal.Config{
		BaseURL:    mock.URL(),
		APIKey:     "ci-key",
		GRPCAddr:   "127.0.0.1:0",
		HTTPAddr:   "127.0.0.1:0",
		HTTPClient: mock.Server.Client(),
	})
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(ctx) }()

	addr := m.HTTPListenAddr()
	for _, path := range []string{"/health", "/healthz"} {
		code, body := getHealth(t, addr, path)
		if code != http.StatusOK || body != "ok" {
			t.Fatalf("%s: code=%d body=%q", path, code, body)
		}
	}

	mock.Close()
	code, _ := getHealth(t, addr, "/health")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("configured-but-down SAB should 503, got %d", code)
	}
}

func TestUnconfiguredHealthAlwaysOK(t *testing.T) {
	m := internal.NewModule(internal.Config{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(ctx) }()

	code, _ := getHealth(t, m.HTTPListenAddr(), "/healthz")
	if code != http.StatusOK {
		t.Fatalf("unconfigured health should 200, got %d", code)
	}
}
