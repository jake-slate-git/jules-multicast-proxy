package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type StreamConfig struct {
	ID                  string `json:"id"`
	StreamName          string `json:"stream_name"`
	SourceMulticastIP   string `json:"source_multicast_ip"`
	SourceMulticastPort int    `json:"source_multicast_port"`
	Enabled             bool   `json:"enabled"`
}

type AppConfig struct {
	AdapterName string         `json:"adapter_name"`
	TargetIPs   []string       `json:"target_ips"`
	DataPort    int            `json:"data_port"`
	Streams     []StreamConfig `json:"streams"`
}

type ConfigManager struct {
	config AppConfig
	mu     sync.RWMutex
	path   string
}

func NewConfigManager() *ConfigManager {
	home, _ := os.UserHomeDir()
	path := filepath.Join(home, ".gox_multicast_sender.json")
	cm := &ConfigManager{
		path: path,
		config: AppConfig{
			DataPort: 6969,
			TargetIPs: []string{},
			Streams: []StreamConfig{},
		},
	}
	cm.Load()
	return cm
}

func (cm *ConfigManager) Load() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	data, err := os.ReadFile(cm.path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &cm.config)
}

func (cm *ConfigManager) Save() error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	data, err := json.MarshalIndent(cm.config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cm.path, data, 0644)
}

func (cm *ConfigManager) GetConfig() AppConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.config
}

func (cm *ConfigManager) UpdateConfig(updater func(*AppConfig)) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	updater(&cm.config)
	go cm.Save()
}
