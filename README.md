# Multicast Proxy System (Stream_Out & Stream_In)

This project provides a peer-to-peer network proxy system to bridge local multicast tactical data (like UAS video or CoT) over standard Layer 3 Unicast VPNs (WireGuard, Tailscale, etc.).

## Components

### 1. Sender (Stream_Out)
- **Purpose**: Runs on the UAS operator's machine.
- **Function**: Listens to local multicast traffic, fans it out to multiple remote VPN IPs via unicast UDP, and hosts a registration server for watchers.
- **Key Features**:
  - Windows Adapter selection by friendly name.
  - Multiple stream management.
  - Heartbeat dashboard (Control Plane status).
  - Configurable data port.

### 2. Receiver (Stream_In)
- **Purpose**: Runs on the watcher's machine.
- **Function**: Periodically registers with the sender to auto-configure. Listens for unicast UDP and rebroadcasts it to the local multicast group.
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

- **Control Plane**: TCP Port 8080 (HTTP/JSON Registration).
- **Data Plane**: Default UDP Port 6969 (Multiplexed).
- **Registration Cycle**: 5 Seconds.
- **WinTAK Aliases**: Saved to `%APPDATA%\GoX_TAK\VideoAliases\` and the local directory.
- **Persistence**: Sender config is stored in `~/.gox_multicast_sender.json`.

## Architecture

1. **Sender** joins multicast groups on a selected interface (including local loopback).
2. **Sender** reads UDP packets, prepends an 8-byte Stream ID, and fans them out to registered watchers.
3. **Receiver** scans the subnet for a Sender on Port 8080 or uses a manually entered IP.
4. **Receiver** registers with the Sender every 5 seconds to receive the latest configuration.
5. **Receiver** sends periodic UDP keep-alives to the Sender to maintain firewall state.
6. **Receiver** listens on the Data Port and rebroadcasts received packets to the local network/loopback.
