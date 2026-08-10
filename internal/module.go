package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/pkg/contracts"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	"github.com/Muxcore-Media/downloader-sabnzbd/internal/sabnzbd"
	usenetv1 "github.com/Muxcore-Media/downloader-sabnzbd/proto/gen/muxcore/usenet/v1"
)

type Module struct {
	id       string
	grpcAddr string
	httpAddr string

	cfgMu  sync.RWMutex
	base   string
	apiKey string

	client  *sabnzbd.Client
	grpcSrv *grpc.Server
	lis     net.Listener
	httpSrv *http.Server
}

type Config struct {
	ID       string
	BaseURL  string
	APIKey   string
	GRPCAddr string
	HTTPAddr string
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
	m := &Module{
		id:       cfg.ID,
		grpcAddr: cfg.GRPCAddr,
		httpAddr: cfg.HTTPAddr,
		base:     cfg.BaseURL,
		apiKey:   cfg.APIKey,
	}
	m.rebuildClient()
	return m
}

func (m *Module) rebuildClient() {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	m.client = &sabnzbd.Client{BaseURL: m.base, APIKey: m.apiKey}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:           m.id,
		Name:         "SABnzbd / Usenet Downloader",
		Version:      "0.1.0",
		Roles:        []string{"downloader", "usenet"},
		Description:  "SABnzbd HTTP API bridge for NZB/usenet downloads",
		Capabilities: []string{"downloader", "downloader.usenet", "usenet", "settings"},
		HTTPAddr:     m.grpcAddr,
	}
}

func (m *Module) Init(ctx context.Context) error { return nil }

func (m *Module) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", m.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", m.grpcAddr, err)
	}
	m.lis = lis
	m.grpcSrv = grpc.NewServer()
	usenetv1.RegisterUsenetDownloaderServiceServer(m.grpcSrv, &usenetServer{m: m})
	modulesdk.RegisterSettings(m.grpcSrv, m.id, m)

	go func() {
		slog.Info("usenet gRPC listening", "addr", m.grpcAddr)
		if err := m.grpcSrv.Serve(lis); err != nil {
			slog.Error("gRPC serve", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	m.httpSrv = &http.Server{Addr: m.httpAddr, Handler: mux}
	go func() {
		slog.Info("health listening", "addr", m.httpAddr)
		if err := m.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health serve", "error", err)
		}
	}()
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	if m.httpSrv != nil {
		_ = m.httpSrv.Shutdown(ctx)
	}
	return nil
}

func (m *Module) Health(ctx context.Context) error {
	m.cfgMu.RLock()
	base, key := m.base, m.apiKey
	m.cfgMu.RUnlock()
	if base == "" || key == "" {
		return nil // unconfigured optional peer
	}
	_, err := m.client.Queue(ctx)
	return err
}

type usenetServer struct {
	usenetv1.UnimplementedUsenetDownloaderServiceServer
	m *Module
}

func (s *usenetServer) AddNZB(ctx context.Context, req *usenetv1.AddNZBRequest) (*usenetv1.AddNZBResponse, error) {
	id, err := s.m.client.AddURL(ctx, req.GetNzbUrl(), req.GetName(), req.GetCategory(), req.GetPaused())
	if err != nil {
		return nil, err
	}
	name := req.GetName()
	if name == "" {
		name = req.GetNzbUrl()
	}
	return &usenetv1.AddNZBResponse{JobId: id, Name: name}, nil
}

func (s *usenetServer) ListQueue(ctx context.Context, req *usenetv1.ListQueueRequest) (*usenetv1.ListQueueResponse, error) {
	items, err := s.m.client.Queue(ctx)
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
	if err := s.m.client.Pause(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &usenetv1.PauseResponse{Success: true}, nil
}

func (s *usenetServer) Resume(ctx context.Context, req *usenetv1.ResumeRequest) (*usenetv1.ResumeResponse, error) {
	if err := s.m.client.Resume(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &usenetv1.ResumeResponse{Success: true}, nil
}

func (s *usenetServer) Delete(ctx context.Context, req *usenetv1.DeleteRequest) (*usenetv1.DeleteResponse, error) {
	if err := s.m.client.Delete(ctx, req.GetId(), req.GetDeleteFiles()); err != nil {
		return nil, err
	}
	return &usenetv1.DeleteResponse{Success: true}, nil
}

func (s *usenetServer) GetHistory(ctx context.Context, req *usenetv1.GetHistoryRequest) (*usenetv1.GetHistoryResponse, error) {
	items, err := s.m.client.History(ctx, int(req.GetLimit()))
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
		SupportsCategories:  true,
		SupportsPausing:     true,
		SupportedProtocols:  []string{"nzb", "usenet"},
		Backend:             "sabnzbd",
	}, nil
}
