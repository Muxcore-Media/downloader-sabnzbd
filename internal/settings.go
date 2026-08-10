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
	return []contracts.SettingDef{
		{
			Key: "base_url", Label: "SABnzbd URL", Type: contracts.SettingTypeString,
			Value: m.base, Description: "e.g. http://127.0.0.1:8080", Group: "Connection",
		},
		{
			Key: "api_key", Label: "API key", Type: contracts.SettingTypeSecret,
			Value: key, Description: "SABnzbd API key", Group: "Connection",
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
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
	m.client = &sabnzbd.Client{BaseURL: m.base, APIKey: m.apiKey}
	return nil
}
