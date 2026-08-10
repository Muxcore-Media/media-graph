package internal

import (
	"fmt"

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
	}
}

func (m *Module) UpdateSetting(key, value string) error {
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	switch key {
	case "default_rel":
		if value == "" {
			return fmt.Errorf("default_rel must not be empty")
		}
		m.defaultRel = value
	case "auto_link":
		m.autoLink = value == "1" || value == "true" || value == "TRUE"
	case "db_path":
		if value == "" {
			return fmt.Errorf("db_path must not be empty")
		}
		m.dbPath = value
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
	return nil
}
