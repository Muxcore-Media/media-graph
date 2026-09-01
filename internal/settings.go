package internal

import (
	"fmt"
	"time"

	"context"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func (m *Module) Settings() []contracts.SettingDef {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return []contracts.SettingDef{
		{Key: "default_rel", Label: "Default relationship", Type: contracts.SettingTypeString,
			Value: m.defaultRel, Default: "related_to",
			Description: "Rel used when Link omits rel; GRAPH_DEFAULT_REL", Group: "Graph"},
		{Key: "auto_link", Label: "Auto-link same title", Type: contracts.SettingTypeBool,
			Value: fmt.Sprintf("%t", m.autoLink), Default: "true",
			Description: "Link same-title nodes across kinds as same_franchise; GRAPH_AUTO_LINK", Group: "Graph"},
		{Key: "db_path", Label: "SQLite path", Type: contracts.SettingTypeString,
			Value: m.dbPath, Description: "Durable graph DB; GRAPH_DB_PATH (restart to apply)", Group: "Graph"},
		{Key: "fixture_path", Label: "Offline fixture path", Type: contracts.SettingTypeString,
			Value: m.fixturePath, Description: "Load movies/TV fixtures at boot; GRAPH_FIXTURE_PATH", Group: "Graph"},
		{Key: "ingest_enabled", Label: "Library ingest", Type: contracts.SettingTypeBool,
			Value: fmt.Sprintf("%t", m.ingestEnabled), Default: "true",
			Description: "Poll + event ingest from library modules; GRAPH_INGEST_ENABLED", Group: "Ingest"},
		{Key: "ingest_interval", Label: "Ingest interval", Type: contracts.SettingTypeString,
			Value: m.ingestInterval.String(), Default: "15m",
			Description: "Poll period; GRAPH_INGEST_INTERVAL", Group: "Ingest"},
	}
}

func (m *Module) UpdateSetting(key, value string) error {
	m.cfgMu.Lock()
	wasEnabled := m.ingestEnabled
	switch key {
	case "default_rel":
		if value == "" {
			m.cfgMu.Unlock()
			return fmt.Errorf("default_rel must not be empty")
		}
		m.defaultRel = value
	case "auto_link":
		m.autoLink = value == "1" || value == "true" || value == "TRUE"
	case "db_path":
		if value == "" {
			m.cfgMu.Unlock()
			return fmt.Errorf("db_path must not be empty")
		}
		m.dbPath = value
	case "fixture_path":
		m.fixturePath = value
	case "ingest_enabled":
		m.ingestEnabled = value == "1" || value == "true" || value == "TRUE"
	case "ingest_interval":
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			m.cfgMu.Unlock()
			return fmt.Errorf("ingest_interval must be a positive duration")
		}
		m.ingestInterval = d
	default:
		m.cfgMu.Unlock()
		return fmt.Errorf("unknown setting %q", key)
	}
	nowEnabled := m.ingestEnabled
	m.cfgMu.Unlock()

	if key == "ingest_enabled" {
		if nowEnabled && !wasEnabled {
			go m.ensureIngestLoops(context.Background())
		} else if !nowEnabled && wasEnabled {
			go m.stopIngest()
		}
	}
	return nil
}
