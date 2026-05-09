package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/ipv4"
)

type NetworkManager struct {
	cm        *ConfigManager
	statusMu  sync.RWMutex
	Watchers  map[string]time.Time
	PacketsSent  int64
	BytesSent    int64
	isRunning    bool
	cancelFunc   context.CancelFunc
}

func NewNetworkManager(cm *ConfigManager) *NetworkManager {
	return &NetworkManager{
		cm:           cm,
		Watchers:     make(map[string]time.Time),
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

	// Start Control Plane Server
	go nm.runControlServer(ctx)

	// Start Watcher Cleanup routine
	go nm.watcherCleanupLoop(ctx)

	// Start a Data Plane routine for each enabled stream
	for _, stream := range config.Streams {
		if stream.Enabled {
			go nm.runStream(ctx, stream, config.AdapterName)
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

func (nm *NetworkManager) runControlServer(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		ip := strings.Split(r.RemoteAddr, ":")[0]
		nm.statusMu.Lock()
		nm.Watchers[ip] = time.Now()
		nm.statusMu.Unlock()

		config := nm.cm.GetConfig()
		payload := HeartbeatPayload{
			Streams:  config.Streams,
			DataPort: config.DataPort,
		}
		json.NewEncoder(w).Encode(payload)
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	fmt.Println("Control Plane listening on :8080")
	server.ListenAndServe()
}

func (nm *NetworkManager) watcherCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nm.statusMu.Lock()
			for ip, lastSeen := range nm.Watchers {
				if time.Since(lastSeen) > 15*time.Second {
					delete(nm.Watchers, ip)
				}
			}
			nm.statusMu.Unlock()
		}
	}
}

func (nm *NetworkManager) runStream(ctx context.Context, stream StreamConfig, adapterName string) {
	ifi, err := net.InterfaceByName(adapterName)
	if err != nil {
		fmt.Printf("Error finding interface %s: %v\n", adapterName, err)
		return
	}

	// For Windows/Cross-platform socket reuse, we must use raw control on the listener.
	// This ensures multiple drones can share the same source port on different multicast IPs.
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var err error
			c.Control(func(fd uintptr) {
				err = setReuseAddr(fd)
			})
			return err
		},
	}

	c, err := lc.ListenPacket(ctx, "udp4", fmt.Sprintf("0.0.0.0:%d", stream.SourceMulticastPort))
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

	// Prepare binary header (ID is already a hex string of 8 chars/4 bytes)
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

			// Update target addresses from dynamic watchers AND manual config
			config := nm.cm.GetConfig()
			nm.statusMu.RLock()
			var combinedTargets []string
			combinedTargets = append(combinedTargets, config.TargetIPs...)
			for ip := range nm.Watchers {
				found := false
				for _, mt := range config.TargetIPs {
					if mt == ip {
						found = true
						break
					}
				}
				if !found {
					combinedTargets = append(combinedTargets, ip)
				}
			}
			nm.statusMu.RUnlock()

			if !equalStrings(lastTargets, combinedTargets) || lastPort != config.DataPort {
				targetAddrs = nil
				for _, t := range combinedTargets {
					addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", t, config.DataPort))
					if err == nil {
						targetAddrs = append(targetAddrs, addr)
					}
				}
				lastTargets = combinedTargets
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
