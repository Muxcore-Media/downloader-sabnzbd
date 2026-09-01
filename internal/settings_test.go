package internal

import (
	"testing"

	"github.com/Muxcore-Media/downloader-sabnzbd/internal/sabnzbd"
)

func TestUpdateSettingMaskAndRebuild(t *testing.T) {
	mock := sabnzbd.NewMockServer("old-key")
	defer mock.Close()

	m := NewModule(Config{
		BaseURL:    mock.URL(),
		APIKey:     "old-key",
		HTTPClient: mock.Server.Client(),
	})

	defs := m.Settings()
	if defs[1].Value != "********" {
		t.Fatalf("api_key should be masked, got %q", defs[1].Value)
	}
	if defs[2].Value != "live" {
		t.Fatalf("engine=%q", defs[2].Value)
	}

	newURL := mock.URL()
	if err := m.UpdateSetting("base_url", newURL); err != nil {
		t.Fatal(err)
	}
	if err := m.UpdateSetting("api_key", "old-key"); err != nil {
		t.Fatal(err)
	}
	if got := m.Settings()[0].Value; got != newURL {
		t.Fatalf("base_url=%q", got)
	}
	if err := m.Health(t.Context()); err != nil {
		t.Fatalf("rebuilt client should reach mock: %v", err)
	}
	if err := m.UpdateSetting("nope", "x"); err == nil {
		t.Fatal("expected unknown key error")
	}
	if err := m.UpdateSetting("engine", "live"); err == nil {
		t.Fatal("expected engine controlled error")
	}
	if err := m.UpdateSetting("api_key", "********"); err != nil {
		t.Fatal(err)
	}
}

func TestFixtureSettingsEngine(t *testing.T) {
	m := NewModule(Config{Fixture: true})
	if got := m.Settings()[2].Value; got != "fixture" {
		t.Fatalf("engine=%q", got)
	}
}
