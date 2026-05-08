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

// Binary Header: 8 bytes for Stream ID (hashed) + payload
const StreamIDHeaderSize = 8

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

	if err := p.SetMulticastLoopback(true); err != nil {
		fmt.Printf("Error setting multicast loopback: %v\n", err)
	}

	// Prepare binary header (using first 8 bytes of ID as a simple unique prefix)
	header := make([]byte, StreamIDHeaderSize)
	copy(header, stream.ID)

	buf := make([]byte, 65535)
	outBuf := make([]byte, 65535+StreamIDHeaderSize)
	copy(outBuf, header)

	outConn, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		fmt.Printf("Error creating output connection: %v\n", err)
		return
	}
	defer outConn.Close()

	var lastTargets []string
	var targetAddrs []*net.UDPAddr
	var lastPort int

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

			// Update target addresses only if config changed
			config := nm.cm.GetConfig()
			if !equalStrings(lastTargets, config.TargetIPs) || lastPort != config.DataPort {
				targetAddrs = nil
				for _, t := range config.TargetIPs {
					addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", t, config.DataPort))
					if err == nil {
						targetAddrs = append(targetAddrs, addr)
					}
				}
				lastTargets = config.TargetIPs
				lastPort = config.DataPort
			}

			nm.statusMu.Lock()
			nm.PacketsSent += int64(len(targetAddrs))
			nm.BytesSent += int64(n * len(targetAddrs))
			nm.statusMu.Unlock()

			// Encapsulate with binary header
			copy(outBuf[StreamIDHeaderSize:], buf[:n])
			totalSize := n + StreamIDHeaderSize

			for _, addr := range targetAddrs {
				outConn.WriteTo(outBuf[:totalSize], addr)
			}
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func GetInterfaces() []string {
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
					mask := ipnet.Mask
					ones, _ := mask.Size()
					res = append(res, fmt.Sprintf("%s (%s/%d)", i.Name, ipnet.IP.String(), ones))
				}
			}
		}
	}
	return res
}
