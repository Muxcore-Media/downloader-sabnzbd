package internal

import (
	"fmt"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/Muxcore-Media/downloader-sabnzbd/internal/sabnzbd"
)

func (m *Module) Settings() []contracts.SettingDef {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	key := m.apiKey
	if key != "" {
		key = "********"
	}
	mode := "live"
	if m.fixture {
		mode = "fixture"
	}
	return []contracts.SettingDef{
		{
			Key: "base_url", Label: "SABnzbd URL", Type: contracts.SettingTypeString,
			Value: m.base, Description: "e.g. http://127.0.0.1:8080 (operator opt-in; ignored when fixture)", Group: "Connection",
		},
		{
			Key: "api_key", Label: "API key", Type: contracts.SettingTypeSecret,
			Value: key, Description: "SABnzbd API key (operator opt-in; never required for CI)", Group: "Connection",
		},
		{
			Key: "engine", Label: "Engine", Type: contracts.SettingTypeString,
			Value: mode, Description: "fixture when SABNZBD_FIXTURE=1 or DOWNLOADER_ENGINE=fixture", Group: "Connection",
		},
	}
}

func (m *Module) UpdateSetting(key, value string) error {
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	switch key {
	case "base_url":
		m.base = value
	case "api_key":
		if value != "" && value != "********" {
			m.apiKey = value
		}
	case "engine":
		return fmt.Errorf("engine is controlled by SABNZBD_FIXTURE / DOWNLOADER_ENGINE")
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
	if !m.fixture {
		c := &sabnzbd.Client{BaseURL: m.base, APIKey: m.apiKey}
		if cl, ok := m.api.(*sabnzbd.Client); ok && cl.HTTPClient != nil {
			c.HTTPClient = cl.HTTPClient
		}
		m.api = c
	}
	return nil
}
