package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/Muxcore-Media/core/sdk/go/client"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	"github.com/Muxcore-Media/downloader-sabnzbd/internal/sabnzbd"
	usenetv1 "github.com/Muxcore-Media/downloader-sabnzbd/proto/gen/muxcore/usenet/v1"
)

const moduleVersion = "0.1.1"

// EventPublisher emits download.* domain events (test sink or mesh adapter).
type EventPublisher func(ctx context.Context, eventType string, payload []byte) error

type Module struct {
	api       sabnzbd.API
	lis       net.Listener
	publish   EventPublisher
	grpcSrv   *grpc.Server
	mc        *client.Client
	httpSrv   *http.Server
	watched   sync.Map // jobID -> jobName
	id        string
	httpAddr  string
	grpcAddr  string
	apiKey    string
	base      string
	pollEvery time.Duration
	pubMu     sync.RWMutex
	cfgMu     sync.RWMutex
	fixture   bool
}

type Config struct {
	Publish    EventPublisher
	HTTPClient *http.Client // optional; for tests inject httptest transport
	ID         string
	BaseURL    string
	APIKey     string
	GRPCAddr   string
	HTTPAddr   string
	PollEvery  time.Duration
	Fixture    bool
}

func fixtureEnabled(explicit bool) bool {
	if explicit {
		return true
	}
	if os.Getenv("SABNZBD_FIXTURE") == "1" || strings.EqualFold(os.Getenv("SABNZBD_FIXTURE"), "true") {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DOWNLOADER_ENGINE"))) {
	case "fixture", "fake":
		return true
	}
	return false
}

func NewModule(cfg Config) *Module {
	if cfg.ID == "" {
		cfg.ID = "downloader-sabnzbd"
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = ":9620"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":9621"
	}
	if v := os.Getenv("SABNZBD_URL"); v != "" {
		cfg.BaseURL = v
	}
	if v := os.Getenv("SABNZBD_API_KEY"); v != "" {
		cfg.APIKey = v
	}
	if v := os.Getenv("MUXCORE_GRPC_ADDR_OVERRIDE"); v != "" {
		cfg.GRPCAddr = v
	}
	if v := os.Getenv("MUXCORE_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	poll := cfg.PollEvery
	if poll <= 0 {
		poll = 2 * time.Second
	}
	useFixture := fixtureEnabled(cfg.Fixture)
	m := &Module{
		id:        cfg.ID,
		grpcAddr:  cfg.GRPCAddr,
		httpAddr:  cfg.HTTPAddr,
		base:      cfg.BaseURL,
		apiKey:    cfg.APIKey,
		fixture:   useFixture,
		publish:   cfg.Publish,
		pollEvery: poll,
	}
	if useFixture {
		m.api = sabnzbd.NewFixtureClient()
		slog.Info("using fixture SABnzbd backend (no live SAB)")
	} else {
		m.rebuildAPI(cfg.HTTPClient)
	}
	return m
}

func (m *Module) rebuildAPI(httpClient *http.Client) {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	if m.fixture {
		return
	}
	c := &sabnzbd.Client{BaseURL: m.base, APIKey: m.apiKey}
	if httpClient != nil {
		c.HTTPClient = httpClient
	} else if cl, ok := m.api.(*sabnzbd.Client); ok && cl.HTTPClient != nil {
		c.HTTPClient = cl.HTTPClient
	}
	m.api = c
}

func (m *Module) SetPublisher(p EventPublisher) {
	m.pubMu.Lock()
	m.publish = p
	m.pubMu.Unlock()
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:           m.id,
		Name:         "SABnzbd / Usenet Downloader",
		Version:      moduleVersion,
		Roles:        []string{"downloader", "usenet"},
		Description:  "SABnzbd HTTP API bridge for NZB/usenet downloads",
		Capabilities: []string{"downloader", "downloader.usenet", "usenet", "settings"},
		HTTPAddr:     m.httpAddr,
	}
}

func (m *Module) Init(ctx context.Context) error { return nil }

func (m *Module) serveHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := m.Health(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (m *Module) Start(ctx context.Context) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", m.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", m.grpcAddr, err)
	}
	m.lis = lis
	m.grpcSrv = grpc.NewServer()
	usenetv1.RegisterUsenetDownloaderServiceServer(m.grpcSrv, &usenetServer{m: m})
	modulesdk.RegisterSettings(m.grpcSrv, m.id, m)

	go func() {
		slog.Info("usenet gRPC listening", "addr", m.grpcAddr, "fixture", m.fixture)
		if serveErr := m.grpcSrv.Serve(lis); serveErr != nil {
			slog.Error("gRPC serve", "error", serveErr)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", m.serveHealth)
	mux.HandleFunc("/healthz", m.serveHealth)
	m.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	httpLis, err := lc.Listen(ctx, "tcp", m.httpAddr)
	if err != nil {
		return fmt.Errorf("listen health %s: %w", m.httpAddr, err)
	}
	m.httpAddr = httpLis.Addr().String()
	go func() {
		slog.Info("health listening", "addr", m.httpAddr)
		if err := m.httpSrv.Serve(httpLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("health serve", "error", err)
		}
	}()
	go m.dialCore(context.WithoutCancel(ctx))
	return nil
}

// ListenAddr returns the bound gRPC address after Start (useful for tests with :0).
func (m *Module) ListenAddr() string {
	if m.lis == nil {
		return m.grpcAddr
	}
	return m.lis.Addr().String()
}

// HTTPListenAddr returns the bound health HTTP address after Start.
func (m *Module) HTTPListenAddr() string {
	return m.httpAddr
}

func (m *Module) Stop(ctx context.Context) error {
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	if m.httpSrv != nil {
		_ = m.httpSrv.Shutdown(ctx)
	}
	if m.mc != nil {
		_ = m.mc.Close()
	}
	return nil
}

func (m *Module) dialCore(ctx context.Context) {
	meshAddr := os.Getenv("MUXCORE_GRPC_ADDR")
	if meshAddr == "" {
		return
	}
	insecureMode := os.Getenv("MUXCORE_INSECURE_DISABLE_TLS") == "true" || os.Getenv("MUXCORE_GRPC_INSECURE") == "true"
	var opts []client.Option
	if insecureMode {
		opts = append(opts, client.WithInsecure())
	}
	c, err := client.Dial(meshAddr, opts...)
	if err != nil {
		slog.Warn("sabnzbd: dial core failed", "error", err)
		return
	}
	m.mc = c
	m.SetPublisher(func(ctx context.Context, eventType string, payload []byte) error {
		return c.Events.Publish(ctx, eventType, m.id, payload)
	})
	slog.Info("sabnzbd: connected to core mesh", "addr", meshAddr)
}

func (m *Module) Health(ctx context.Context) error {
	if m.fixture {
		return nil
	}
	m.cfgMu.RLock()
	base, key := m.base, m.apiKey
	m.cfgMu.RUnlock()
	if base == "" || key == "" {
		return nil // unconfigured optional peer
	}
	_, err := m.api.Queue(ctx)
	return err
}

func (m *Module) configured() error {
	if m.fixture {
		return nil
	}
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	if m.base == "" || m.apiKey == "" {
		return fmt.Errorf("sabnzbd unconfigured: set SABNZBD_URL + SABNZBD_API_KEY (or SABNZBD_FIXTURE=1 / DOWNLOADER_ENGINE=fixture)")
	}
	return nil
}

func (m *Module) queueJobName(ctx context.Context, id string) string {
	items, err := m.api.Queue(ctx)
	if err != nil {
		return ""
	}
	for _, it := range items {
		if it.ID == id {
			return it.Name
		}
	}
	return ""
}

// OfflineDispatch queues an NZB against the configured (or mock) SABnzbd and
// waits until history reports Completed/Failed. Used by offline automation
// paths and unit tests — never hits a live usenet provider by itself.
func (m *Module) OfflineDispatch(ctx context.Context, nzbURL, name, category string) (jobID string, err error) {
	if cfgErr := m.configured(); cfgErr != nil {
		return "", cfgErr
	}
	id, err := m.api.AddURL(ctx, nzbURL, name, category, false)
	if err != nil {
		return "", err
	}
	if name == "" {
		name = nzbURL
	}
	m.publishDownload(ctx, contracts.EventDownloadStarted, id, name, "", "", nil)
	m.watchJob(ctx, id, name)
	return id, m.waitHistory(ctx, id)
}

// watchJob polls history for id until it completes, fails, or 72h elapse. The
// watcher outlives the calling request, so it detaches from ctx cancellation
// while keeping its values.
func (m *Module) watchJob(ctx context.Context, id, name string) {
	if id == "" {
		return
	}
	if _, loaded := m.watched.LoadOrStore(id, name); loaded {
		return
	}
	detached := context.WithoutCancel(ctx)
	go func() {
		ticker := time.NewTicker(m.pollEvery)
		defer ticker.Stop()
		defer m.watched.Delete(id)
		deadline := time.Now().Add(72 * time.Hour)
		for time.Now().Before(deadline) {
			pollCtx, cancel := context.WithTimeout(detached, 5*time.Second)
			h, err := m.api.HistoryJob(pollCtx, id)
			cancel()
			if err == nil && h != nil {
				st := strings.ToLower(h.Status)
				switch {
				case strings.Contains(st, "complete"):
					files := m.historyFiles(detached, id)
					m.publishDownload(detached, contracts.EventDownloadCompleted, h.ID, h.Name, h.Storage, "", files)
					return
				case strings.Contains(st, "fail"):
					m.publishDownload(detached, contracts.EventDownloadFailed, h.ID, h.Name, h.Storage, h.Status, nil)
					return
				}
			}
			<-ticker.C
		}
		jobName := name
		if jobName == "" {
			if v, ok := m.watched.Load(id); ok {
				jobName, _ = v.(string)
			}
		}
		m.publishDownload(detached, contracts.EventDownloadFailed, id, jobName, "", "watch deadline exceeded (72h)", nil)
	}()
}

func (m *Module) waitHistory(ctx context.Context, id string) error {
	ticker := time.NewTicker(m.pollEvery)
	defer ticker.Stop()
	for {
		h, err := m.api.HistoryJob(ctx, id)
		if err != nil {
			return err
		}
		if h != nil {
			st := strings.ToLower(h.Status)
			if strings.Contains(st, "complete") {
				return nil
			}
			if strings.Contains(st, "fail") {
				return fmt.Errorf("sabnzbd job %s failed: %s", id, h.Status)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Module) historyFiles(ctx context.Context, id string) []contracts.DownloadEventFile {
	files, err := m.api.HistoryFiles(ctx, id)
	if err != nil || len(files) == 0 {
		return nil
	}
	out := make([]contracts.DownloadEventFile, 0, len(files))
	for _, f := range files {
		out = append(out, contracts.DownloadEventFile{Path: f.Path, Size: f.Size})
	}
	return out
}

func (m *Module) publishDownload(ctx context.Context, eventType, id, name, savePath, errStr string, files []contracts.DownloadEventFile) {
	m.pubMu.RLock()
	pub := m.publish
	m.pubMu.RUnlock()
	if pub == nil {
		return
	}
	if len(files) == 0 && savePath != "" {
		files = []contracts.DownloadEventFile{{Path: savePath}}
	}
	payload, err := json.Marshal(contracts.DownloadEventPayload{
		ID: id, Name: name, SavePath: savePath, Label: "usenet", Error: errStr,
		Files: files,
	})
	if err != nil {
		return
	}
	// Publish even if the caller's ctx is already cancelled (e.g. failure events).
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := pub(ctx, eventType, payload); err != nil {
		slog.Warn("sabnzbd: publish event failed", "type", eventType, "error", err)
	}
}

type usenetServer struct {
	usenetv1.UnimplementedUsenetDownloaderServiceServer
	m *Module
}

func (s *usenetServer) addNZBJob(ctx context.Context, req *usenetv1.AddNZBRequest) (id, name string, err error) {
	if len(req.GetNzbContent()) > 0 {
		id, err = s.m.api.AddFile(ctx, req.GetNzbContent(), req.GetName(), req.GetCategory(), req.GetPaused())
	} else {
		id, err = s.m.api.AddURL(ctx, req.GetNzbUrl(), req.GetName(), req.GetCategory(), req.GetPaused())
	}
	if err != nil {
		return "", "", err
	}
	name = req.GetName()
	if name == "" {
		if len(req.GetNzbContent()) > 0 {
			name = "upload.nzb"
		} else {
			name = req.GetNzbUrl()
		}
	}
	return id, name, nil
}

func (s *usenetServer) AddNZB(ctx context.Context, req *usenetv1.AddNZBRequest) (*usenetv1.AddNZBResponse, error) {
	if err := s.m.configured(); err != nil {
		return nil, err
	}
	if len(req.GetNzbContent()) == 0 && req.GetNzbUrl() == "" {
		return nil, fmt.Errorf("nzb_url or nzb_content required")
	}
	id, name, err := s.addNZBJob(ctx, req)
	if err != nil {
		return nil, err
	}
	s.m.publishDownload(ctx, contracts.EventDownloadStarted, id, name, "", "", nil)
	if !req.GetPaused() {
		s.m.watchJob(ctx, id, name)
	}
	return &usenetv1.AddNZBResponse{JobId: id, Name: name}, nil
}

func (s *usenetServer) ListQueue(ctx context.Context, req *usenetv1.ListQueueRequest) (*usenetv1.ListQueueResponse, error) {
	if err := s.m.configured(); err != nil {
		return &usenetv1.ListQueueResponse{}, nil //nolint:nilerr // unconfigured backend reports an empty queue, not an RPC error
	}
	items, err := s.m.api.Queue(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*usenetv1.QueueItem, 0, len(items))
	for _, it := range items {
		if cat := req.GetCategory(); cat != "" && it.Category != cat {
			continue
		}
		out = append(out, &usenetv1.QueueItem{
			Id:        it.ID,
			Name:      it.Name,
			Status:    it.Status,
			Category:  it.Category,
			Progress:  it.Percentage,
			Size:      int64(it.SizeMB * 1024 * 1024),
			Remaining: int64(it.Remaining * 1024 * 1024),
		})
	}
	return &usenetv1.ListQueueResponse{Items: out}, nil
}

func (s *usenetServer) Pause(ctx context.Context, req *usenetv1.PauseRequest) (*usenetv1.PauseResponse, error) {
	if err := s.m.configured(); err != nil {
		return nil, err
	}
	if err := s.m.api.Pause(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &usenetv1.PauseResponse{Success: true}, nil
}

func (s *usenetServer) Resume(ctx context.Context, req *usenetv1.ResumeRequest) (*usenetv1.ResumeResponse, error) {
	if err := s.m.configured(); err != nil {
		return nil, err
	}
	id := req.GetId()
	name := s.m.queueJobName(ctx, id)
	if err := s.m.api.Resume(ctx, id); err != nil {
		return nil, err
	}
	if id != "" {
		s.m.watchJob(ctx, id, name)
	}
	return &usenetv1.ResumeResponse{Success: true}, nil
}

func (s *usenetServer) Delete(ctx context.Context, req *usenetv1.DeleteRequest) (*usenetv1.DeleteResponse, error) {
	if err := s.m.configured(); err != nil {
		return nil, err
	}
	if err := s.m.api.Delete(ctx, req.GetId(), req.GetDeleteFiles()); err != nil {
		return nil, err
	}
	return &usenetv1.DeleteResponse{Success: true}, nil
}

func (s *usenetServer) GetHistory(ctx context.Context, req *usenetv1.GetHistoryRequest) (*usenetv1.GetHistoryResponse, error) {
	if err := s.m.configured(); err != nil {
		return &usenetv1.GetHistoryResponse{}, nil //nolint:nilerr // unconfigured backend reports empty history, not an RPC error
	}
	items, err := s.m.api.History(ctx, int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	out := make([]*usenetv1.HistoryItem, 0, len(items))
	for _, it := range items {
		out = append(out, &usenetv1.HistoryItem{
			Id: it.ID, Name: it.Name, Status: it.Status, Category: it.Category, Storage: it.Storage,
		})
	}
	return &usenetv1.GetHistoryResponse{Items: out}, nil
}

func (s *usenetServer) GetCapabilities(_ context.Context, _ *usenetv1.GetCapabilitiesRequest) (*usenetv1.GetCapabilitiesResponse, error) {
	return &usenetv1.GetCapabilitiesResponse{
		SupportsCategories: true,
		SupportsPausing:    true,
		SupportedProtocols: []string{"nzb", "usenet"},
		Backend:            "sabnzbd",
	}, nil
}
