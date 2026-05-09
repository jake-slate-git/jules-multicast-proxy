package main

import (
	"encoding/json"
	"os"
	"testing"
)

func TestConfigPersistence(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "config_test*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	cm := &ConfigManager{
		path: tmpfile.Name(),
	}

	cm.UpdateConfig(func(c *AppConfig) {
		c.AdapterName = "TestAdapter"
		c.DataPort = 1234
		c.Streams = []StreamConfig{
			{ID: "1", StreamName: "TestDrone", Enabled: true},
		}
	})

	// Force synchronous save for test
	err = cm.Save()
	if err != nil {
		t.Fatal(err)
	}

	err = cm.Load()
	if err != nil {
		t.Errorf("Load failed: %v", err)
	}

	config := cm.GetConfig()
	if config.AdapterName != "TestAdapter" {
		t.Errorf("Expected TestAdapter, got %s", config.AdapterName)
	}
	if len(config.Streams) != 1 {
		t.Errorf("Expected 1 stream, got %d", len(config.Streams))
	}
}

func TestHeartbeatPayload(t *testing.T) {
	streams := []StreamConfig{
		{ID: "1", StreamName: "Drone1", Enabled: true},
	}
	payload := HeartbeatPayload{
		Streams:  streams,
		DataPort: 6969,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}

	var decoded HeartbeatPayload
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.DataPort != 6969 {
		t.Errorf("Expected DataPort 6969, got %d", decoded.DataPort)
	}
}
