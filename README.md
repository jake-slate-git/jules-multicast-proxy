# Multicast Proxy System (Stream_Out & Stream_In)

This project provides a peer-to-peer network proxy system to bridge local multicast tactical data (like UAS video or CoT) over standard Layer 3 Unicast VPNs (WireGuard, Tailscale, etc.).

## Components

### 1. Sender (Stream_Out)
- **Purpose**: Runs on the UAS operator's machine.
- **Function**: Listens to local multicast traffic, fans it out to multiple remote VPN IPs via unicast UDP, and sends periodic heartbeats to configure receivers.
- **Key Features**:
  - Windows Adapter selection by friendly name.
  - Multiple stream management.
  - Heartbeat dashboard (Control Plane status).
  - Configurable data port.

### 2. Receiver (Stream_In)
- **Purpose**: Runs on the watcher's machine.
- **Function**: Receives heartbeats from the sender to auto-configure. Listens for unicast UDP and rebroadcasts it to the local multicast group.
- **Key Features**:
  - Zero-touch configuration.
  - WinTAK Video Alias integration (automatic XML generation).
  - Real-time traffic stats.

## How to Build (Windows)

### Prerequisites
1. [Go (Golang)](https://go.dev/dl/) installed.
2. C compiler for Fyne (on Windows, [MSYS2](https://www.msys2.org/) with `mingw-w64-x86_64-gcc` is recommended).

### Building the Sender
```bash
cd sender
go mod tidy
go build -ldflags="-H windowsgui" -o Stream_Out.exe .
```

### Building the Receiver
```bash
cd receiver
go mod tidy
go build -ldflags="-H windowsgui" -o Stream_In.exe .
```

## Technical Details

- **Control Plane**: TCP Port 8080 (HTTP/JSON).
- **Data Plane**: Default UDP Port 6969 (Configurable).
- **WinTAK Aliases**: Saved to `%APPDATA%\GoX_TAK\VideoAliases\` and the local directory.
- **Persistence**: Sender config is stored in `~/.gox_multicast_sender.json`.

## Architecture

1. **Sender** joins a multicast group on a selected interface.
2. **Sender** reads UDP packets and sends a copy to each Target IP on the Data Port.
3. **Sender** sends a JSON heartbeat every 30 seconds to each Target IP on Port 8080.
4. **Receiver** listens on Port 8080. On heartbeat, it updates which streams to expect and which multicast group to rebroadcast to.
5. **Receiver** listens on the Data Port and rebroadcasts received packets to the local network/loopback.
