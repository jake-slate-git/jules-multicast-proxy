package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestReceiverHandleUpdate(t *testing.T) {
	rm := NewReceiverManager()

	payload := HeartbeatPayload{
		DataPort: 7000,
		Streams: []StreamConfig{
			{ID: "stream1", DroneName: "Drone1", SourceMulticastIP: "239.1.1.1", SourceMulticastPort: 5001, Enabled: true},
		},
	}

	rm.HandleUpdate(payload)

	rm.mu.RLock()
	if rm.DataPort != 7000 {
		t.Errorf("Expected DataPort 7000, got %d", rm.DataPort)
	}
	if len(rm.ActiveStreams) != 1 {
		t.Errorf("Expected 1 active stream, got %d", len(rm.ActiveStreams))
	}
	stream, ok := rm.ActiveStreams["stream1"]
	if !ok {
		t.Fatal("stream1 not found in active streams")
	}
	if stream.Config.DroneName != "Drone1" {
		t.Errorf("Expected Drone1, got %s", stream.Config.DroneName)
	}
	rm.mu.RUnlock()

	// Verify file generation (local)
	if _, err := os.Stat("Drone1.xml"); os.IsNotExist(err) {
		t.Error("Drone1.xml was not generated")
	}
	defer os.Remove("Drone1.xml")
}

func TestReceiverHTTP(t *testing.T) {
	rm := NewReceiverManager()
	server := httptest.NewServer(rm)
	defer server.Close()

	payload := HeartbeatPayload{
		DataPort: 8000,
		Streams:  []StreamConfig{{ID: "s2", DroneName: "D2", Enabled: true}},
	}
	body, _ := json.Marshal(payload)

	resp, err := http.Post(server.URL, "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected OK, got %v", resp.Status)
	}

	rm.mu.RLock()
	if rm.DataPort != 8000 {
		t.Errorf("Expected 8000, got %d", rm.DataPort)
	}
	rm.mu.RUnlock()
	os.Remove("D2.xml")
}
