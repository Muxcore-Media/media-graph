package internal

import (
	"fmt"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func (m *Module) Settings() []contracts.SettingDef {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return []contracts.SettingDef{{
		Key: "default_rel", Label: "Default relationship", Type: contracts.SettingTypeString,
		Value: m.defaultRel, Default: "related_to",
		Description: "Rel used when Link omits rel; GRAPH_DEFAULT_REL", Group: "Graph",
	}}
}

func (m *Module) UpdateSetting(key, value string) error {
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	if key != "default_rel" {
		return fmt.Errorf("unknown setting %q", key)
	}
	if value == "" {
		return fmt.Errorf("default_rel must not be empty")
	}
	m.defaultRel = value
	return nil
}
