package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type StreamConfig struct {
	ID                  string `json:"id"`
	DroneName           string `json:"drone_name"`
	SourceMulticastIP   string `json:"source_multicast_ip"`
	SourceMulticastPort int    `json:"source_multicast_port"`
	Enabled             bool   `json:"enabled"`
}

type HeartbeatPayload struct {
	Streams  []StreamConfig `json:"streams"`
	DataPort int            `json:"data_port"`
}

type DataPacket struct {
	StreamID string `json:"s"`
	Payload  []byte `json:"p"`
}

type StreamState struct {
	Config     StreamConfig
	CancelFunc context.CancelFunc
	LastSeen   time.Time
	Packets    int64
	Bytes      int64
	OutConn    *net.UDPConn
}

type ReceiverManager struct {
	mu            sync.RWMutex
	ActiveStreams map[string]*StreamState
	DataPort      int
	cancelServer  context.CancelFunc
}

func NewReceiverManager() *ReceiverManager {
	return &ReceiverManager{
		ActiveStreams: make(map[string]*StreamState),
	}
}

func (rm *ReceiverManager) HandleUpdate(payload HeartbeatPayload) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	restartServer := false
	if rm.DataPort != payload.DataPort {
		rm.DataPort = payload.DataPort
		restartServer = true
	}

	// Mark all current streams as potentially stale
	receivedIDs := make(map[string]bool)

	for _, s := range payload.Streams {
		if !s.Enabled {
			continue
		}
		receivedIDs[s.ID] = true

		existing, exists := rm.ActiveStreams[s.ID]
		if exists {
			existing.LastSeen = time.Now()
			// If config changed, update connection
			if existing.Config.SourceMulticastIP != s.SourceMulticastIP ||
				existing.Config.SourceMulticastPort != s.SourceMulticastPort {
				if existing.OutConn != nil {
					existing.OutConn.Close()
				}
				existing.Config = s
				existing.OutConn = rm.createOutConn(s)
			}
		} else {
			state := &StreamState{
				Config:   s,
				LastSeen: time.Now(),
				OutConn:  rm.createOutConn(s),
			}
			rm.ActiveStreams[s.ID] = state
			rm.generateWinTAKAlias(s)
		}
	}

	// Cleanup streams not in the latest heartbeat
	for id, state := range rm.ActiveStreams {
		if !receivedIDs[id] {
			if state.OutConn != nil {
				state.OutConn.Close()
			}
			delete(rm.ActiveStreams, id)
		}
	}

	if restartServer || rm.cancelServer == nil {
		if rm.cancelServer != nil {
			rm.cancelServer()
		}
		ctx, cancel := context.WithCancel(context.Background())
		rm.cancelServer = cancel
		go rm.runServer(ctx)
	}
}

func (rm *ReceiverManager) createOutConn(s StreamConfig) *net.UDPConn {
	destAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", s.SourceMulticastIP, s.SourceMulticastPort))
	if err != nil {
		return nil
	}
	outConn, err := net.DialUDP("udp4", nil, destAddr)
	if err != nil {
		return nil
	}
	return outConn
}

func (rm *ReceiverManager) runServer(ctx context.Context) {
	addr := fmt.Sprintf("0.0.0.0:%d", rm.DataPort)
	pc, err := net.ListenPacket("udp4", addr)
	if err != nil {
		fmt.Printf("Error listening for data on port %d: %v\n", rm.DataPort, err)
		return
	}
	defer pc.Close()

	buf := make([]byte, 65535)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			pc.SetReadDeadline(time.Now().Add(time.Second))
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				continue
			}

			var packet DataPacket
			if err := json.Unmarshal(buf[:n], &packet); err != nil {
				continue
			}

			rm.mu.RLock()
			state, ok := rm.ActiveStreams[packet.StreamID]
			if ok && state.OutConn != nil {
				state.Packets++
				state.Bytes += int64(len(packet.Payload))
				state.OutConn.Write(packet.Payload)
			}
			rm.mu.RUnlock()
		}
	}
}

func (rm *ReceiverManager) generateWinTAKAlias(s StreamConfig) {
	xml := fmt.Sprintf(`<video><alias>%s</alias><address>%s</address><port>%d</port><protocol>udp</protocol></video>`,
		s.DroneName, s.SourceMulticastIP, s.SourceMulticastPort)

	filename := fmt.Sprintf("%s.xml", s.DroneName)

	// Local directory
	os.WriteFile(filename, []byte(xml), 0644)

	// AppData directory
	appData := os.Getenv("APPDATA")
	if appData != "" {
		dir := filepath.Join(appData, "GoX_TAK", "VideoAliases")
		os.MkdirAll(dir, 0755)
		path := filepath.Join(dir, filename)
		os.WriteFile(path, []byte(xml), 0644)
	}
}

func (rm *ReceiverManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload HeartbeatPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rm.HandleUpdate(payload)
	w.WriteHeader(http.StatusOK)
}
