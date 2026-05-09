package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

const StreamIDHeaderSize = 8

type StreamState struct {
	Config     StreamConfig
	CancelFunc context.CancelFunc
	LastSeen   time.Time
	LastPacket time.Time
	Packets    int64
	Bytes      int64
	OutConn    *net.UDPConn
}

type ReceiverManager struct {
	mu            sync.RWMutex
	ActiveStreams map[string]*StreamState
	DataPort      int
	cancelServer  context.CancelFunc
	SenderIP      string
	IsRunning     bool
	cancelGlobal  context.CancelFunc
	LastCheckIn   time.Time
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
			if err != nil || n < StreamIDHeaderSize {
				continue
			}

			// Extract Stream ID prefix
			idPrefix := string(buf[:StreamIDHeaderSize])

			rm.mu.RLock()
			targetState, ok := rm.ActiveStreams[idPrefix]

			if ok && targetState.OutConn != nil {
				targetState.Packets++
				targetState.Bytes += int64(n - StreamIDHeaderSize)
				targetState.LastPacket = time.Now()
				targetState.OutConn.Write(buf[StreamIDHeaderSize:n])
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

func (rm *ReceiverManager) Start() {
	rm.mu.Lock()
	if rm.IsRunning {
		rm.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	rm.cancelGlobal = cancel
	rm.IsRunning = true
	rm.mu.Unlock()

	go rm.registrationLoop(ctx)
	go rm.discoveryLoop(ctx)
}

func (rm *ReceiverManager) Stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.cancelGlobal != nil {
		rm.cancelGlobal()
	}
	if rm.cancelServer != nil {
		rm.cancelServer()
	}
	rm.IsRunning = false
}

func (rm *ReceiverManager) registrationLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	rm.checkIn()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rm.checkIn()
		}
	}
}

func (rm *ReceiverManager) checkIn() {
	rm.mu.RLock()
	senderIP := rm.SenderIP
	rm.mu.RUnlock()

	if senderIP == "" {
		return
	}

	url := fmt.Sprintf("http://%s/register", senderIP)
	if !strings.Contains(senderIP, ":") {
		url = fmt.Sprintf("http://%s:8080/register", senderIP)
	}
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", nil)
	if err != nil {
		fmt.Printf("Registration failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var payload HeartbeatPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		fmt.Printf("Failed to decode heartbeat: %v\n", err)
		return
	}

	rm.mu.Lock()
	rm.LastCheckIn = time.Now()
	rm.mu.Unlock()

	rm.HandleUpdate(payload)

	// UDP Hole Punch / Keep-alive
	go rm.sendKeepAlive(senderIP, payload.DataPort)
}

func (rm *ReceiverManager) discoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	rm.scanForSender()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rm.scanForSender()
		}
	}
}

func (rm *ReceiverManager) scanForSender() {
	rm.mu.RLock()
	if rm.SenderIP != "" {
		rm.mu.RUnlock()
		return // Already have an IP
	}
	rm.mu.RUnlock()

	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok || ipnet.IP.IsLoopback() || ipnet.IP.To4() == nil {
				continue
			}

			// Scan first 512 IPs in this subnet
			baseIP := ipnet.IP.Mask(ipnet.Mask).To4()
			for i := 0; i < 512; i++ {
				ip := make(net.IP, 4)
				copy(ip, baseIP)

				// Increment IP logic
				ip[3] += byte(i % 256)
				ip[2] += byte(i / 256)

				if ip.Equal(ipnet.IP) {
					continue
				}

				targetIP := ip.String()
				// Use a small sleep to avoid overwhelming the OS with 512 concurrent network requests
				time.Sleep(2 * time.Millisecond)
				go func(tIP string) {
					url := fmt.Sprintf("http://%s:8080/register", tIP)
					client := http.Client{Timeout: 500 * time.Millisecond}
					resp, err := client.Post(url, "application/json", nil)
					if err == nil {
						resp.Body.Close()
						rm.mu.Lock()
						if rm.SenderIP == "" {
							rm.SenderIP = tIP
							fmt.Printf("Auto-discovered sender at %s\n", tIP)
						}
						rm.mu.Unlock()
					}
				}(targetIP)
			}
		}
	}
}

func (rm *ReceiverManager) sendKeepAlive(ip string, port int) {
	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return
	}
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.Write([]byte("keepalive"))
}
