package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
)

type NetworkManager struct {
	cm        *ConfigManager
	statusMu  sync.RWMutex
	TargetStatus map[string]string
	PacketsSent  int64
	BytesSent    int64
	isRunning    bool
	cancelFunc   context.CancelFunc
}

func NewNetworkManager(cm *ConfigManager) *NetworkManager {
	return &NetworkManager{
		cm:           cm,
		TargetStatus: make(map[string]string),
	}
}

type HeartbeatPayload struct {
	Streams  []StreamConfig `json:"streams"`
	DataPort int            `json:"data_port"`
}

// DataPacket is used to encapsulate UDP data with a stream identifier
type DataPacket struct {
	StreamID string `json:"s"`
	Payload  []byte `json:"p"`
}

func (nm *NetworkManager) Start() {
	nm.statusMu.Lock()
	if nm.isRunning {
		nm.statusMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	nm.cancelFunc = cancel
	nm.isRunning = true
	nm.statusMu.Unlock()

	config := nm.cm.GetConfig()

	// Start Heartbeat routine
	go nm.heartbeatLoop(ctx)

	// Start a Data Plane routine for each enabled stream
	for _, stream := range config.Streams {
		if stream.Enabled {
			go nm.runStream(ctx, stream, config.AdapterName, config.TargetIPs, config.DataPort)
		}
	}
}

func (nm *NetworkManager) Stop() {
	nm.statusMu.Lock()
	defer nm.statusMu.Unlock()
	if nm.cancelFunc != nil {
		nm.cancelFunc()
	}
	nm.isRunning = false
}

func (nm *NetworkManager) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	nm.sendHeartbeats()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nm.sendHeartbeats()
		}
	}
}

func (nm *NetworkManager) sendHeartbeats() {
	config := nm.cm.GetConfig()
	payload := HeartbeatPayload{
		Streams:  config.Streams,
		DataPort: config.DataPort,
	}
	jsonData, _ := json.Marshal(payload)

	for _, target := range config.TargetIPs {
		go func(ip string) {
			url := fmt.Sprintf("http://%s:8080/update", ip)
			client := http.Client{Timeout: 5 * time.Second}
			resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))

			nm.statusMu.Lock()
			if err != nil {
				nm.TargetStatus[ip] = fmt.Sprintf("Error: %v", err)
			} else {
				nm.TargetStatus[ip] = fmt.Sprintf("%s OK", resp.Status)
				resp.Body.Close()
			}
			nm.statusMu.Unlock()
		}(target)
	}
}

func (nm *NetworkManager) runStream(ctx context.Context, stream StreamConfig, adapterName string, targets []string, dataPort int) {
	ifi, err := net.InterfaceByName(adapterName)
	if err != nil {
		fmt.Printf("Error finding interface %s: %v\n", adapterName, err)
		return
	}

	c, err := net.ListenPacket("udp4", fmt.Sprintf("0.0.0.0:%d", stream.SourceMulticastPort))
	if err != nil {
		fmt.Printf("Error listening on port %d: %v\n", stream.SourceMulticastPort, err)
		return
	}
	defer c.Close()

	p := ipv4.NewPacketConn(c)
	group := net.ParseIP(stream.SourceMulticastIP)
	if err := p.JoinGroup(ifi, &net.UDPAddr{IP: group}); err != nil {
		fmt.Printf("Error joining group %s: %v\n", stream.SourceMulticastIP, err)
		return
	}

	// Important for Windows: loopback might need to be disabled or enabled depending on source
	if err := p.SetMulticastLoopback(true); err != nil {
		fmt.Printf("Error setting multicast loopback: %v\n", err)
	}

	buf := make([]byte, 65535)

	outConn, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		fmt.Printf("Error creating output connection: %v\n", err)
		return
	}
	defer outConn.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			c.SetReadDeadline(time.Now().Add(time.Second))
			n, _, _, err := p.ReadFrom(buf)
			if err != nil {
				if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
					continue
				}
				return
			}

			config := nm.cm.GetConfig()
			var targetAddrs []*net.UDPAddr
			for _, t := range config.TargetIPs {
				addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", t, config.DataPort))
				if err == nil {
					targetAddrs = append(targetAddrs, addr)
				}
			}

			nm.statusMu.Lock()
			nm.PacketsSent += int64(len(targetAddrs))
			nm.BytesSent += int64(n * len(targetAddrs))
			nm.statusMu.Unlock()

			// Encapsulate
			packet := DataPacket{
				StreamID: stream.ID,
				Payload:  buf[:n],
			}
			data, _ := json.Marshal(packet)

			for _, addr := range targetAddrs {
				outConn.WriteTo(data, addr)
			}
		}
	}
}

func GetInterfaces() []string {
	// In a real Windows environment, one would use platform-specific APIs
	// or "netsh interface show interface" to get friendly names.
	// For this implementation, we will use net.Interfaces() but include a
	// placeholder for where friendly name logic would integrate.
	ifaces, err := net.Interfaces()
	if err != nil {
		return []string{}
	}
	var res []string
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					// On Windows, i.Name might be {GUID}.
					// A more advanced implementation would use 'golang.org/x/sys/windows'
					// to map Index/Name to Friendly Name.
					res = append(res, fmt.Sprintf("%s (%s)", i.Name, ipnet.IP.String()))
				}
			}
		}
	}
	return res
}
