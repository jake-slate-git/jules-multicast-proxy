package main

import (
	"fmt"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/data/binding"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"
)

func main() {
	a := app.NewWithID("com.jules.multicast.sender")
	w := a.NewWindow("Multicast Stream Sender")

	cm := NewConfigManager()
	nm := NewNetworkManager(cm)

	// UI State
	config := cm.GetConfig()

	// Adapters
	ifaces := GetInterfaces()
	adapterSelect := widget.NewSelect(ifaces, func(s string) {
		name := strings.Split(s, " (")[0]
		cm.UpdateConfig(func(c *AppConfig) {
			c.AdapterName = name
		})
	})
	if config.AdapterName != "" {
		for _, iface := range ifaces {
			if strings.HasPrefix(iface, config.AdapterName) {
				adapterSelect.SetSelected(iface)
				break
			}
		}
	}

	// Targets
	targetsEntry := widget.NewMultiLineEntry()
	targetsEntry.SetText(strings.Join(config.TargetIPs, "\n"))
	targetsEntry.OnChanged = func(s string) {
		lines := strings.Split(s, "\n")
		var ips []string
		for _, l := range lines {
			l = strings.TrimSpace(l)
			if l != "" {
				ips = append(ips, l)
			}
		}
		cm.UpdateConfig(func(c *AppConfig) {
			c.TargetIPs = ips
		})
	}

	// Data Port
	portEntry := widget.NewEntry()
	portEntry.SetText(fmt.Sprintf("%d", config.DataPort))
	portEntry.OnChanged = func(s string) {
		var p int
		fmt.Sscanf(s, "%d", &p)
		if p > 0 {
			cm.UpdateConfig(func(c *AppConfig) {
				c.DataPort = p
			})
		}
	}

	// Stream List
	streamListContainer := container.NewVBox()
	var refreshStreams func()
	refreshStreams = func() {
		streamListContainer.Objects = nil
		conf := cm.GetConfig()
		for i, s := range conf.Streams {
			index := i
			stream := s
			status := "Disabled"
			if stream.Enabled {
				status = "Enabled"
			}

			row := container.NewHBox(
				widget.NewLabel(fmt.Sprintf("%s (%s:%d)", stream.DroneName, stream.SourceMulticastIP, stream.SourceMulticastPort)),
				widget.NewLabel(status),
				widget.NewButton("Delete", func() {
					cm.UpdateConfig(func(c *AppConfig) {
						c.Streams = append(c.Streams[:index], c.Streams[index+1:]...)
					})
					refreshStreams()
				}),
			)
			streamListContainer.Add(row)
		}
		streamListContainer.Refresh()
	}

	// Add Stream Dialog
	addStreamBtn := widget.NewButton("Add Stream", func() {
		droneEntry := widget.NewEntry()
		ipEntry := widget.NewEntry()
		portEntry := widget.NewEntry()

		items := []*widget.FormItem{
			{Text: "Drone Name", Widget: droneEntry},
			{Text: "Multicast IP", Widget: ipEntry},
			{Text: "Multicast Port", Widget: portEntry},
		}

		dialog.ShowForm("Add New Stream", "Add", "Cancel", items, func(b bool) {
			if b {
				var p int
				fmt.Sscanf(portEntry.Text, "%d", &p)
				newStream := StreamConfig{
					ID:                  fmt.Sprintf("%d", time.Now().UnixNano()),
					DroneName:           droneEntry.Text,
					SourceMulticastIP:   ipEntry.Text,
					SourceMulticastPort: p,
					Enabled:             true,
				}
				cm.UpdateConfig(func(c *AppConfig) {
					c.Streams = append(c.Streams, newStream)
				})
				refreshStreams()
			}
		}, w)
	})

	// Control Buttons
	var startBtn *widget.Button
	var stopBtn *widget.Button

	startBtn = widget.NewButton("START STREAMING", func() {
		nm.Start()
		startBtn.Disable()
		stopBtn.Enable()
	})
	stopBtn = widget.NewButton("STOP", func() {
		nm.Stop()
		startBtn.Enable()
		stopBtn.Disable()
	})
	stopBtn.Disable()

	// Stats
	packetsLabel := widget.NewLabel("Packets Sent: 0")
	bytesLabel := widget.NewLabel("Bytes Sent: 0")

	// Simpler status display
	statusText := widget.NewMultiLineEntry()
	statusText.ReadOnly = true

	go func() {
		for {
			time.Sleep(time.Second)
			nm.statusMu.RLock()
			packetsLabel.SetText(fmt.Sprintf("Packets Sent: %d", nm.PacketsSent))
			bytesLabel.SetText(fmt.Sprintf("Bytes Sent: %d KB", nm.BytesSent/1024))

			var sb strings.Builder
			for ip, status := range nm.TargetStatus {
				sb.WriteString(fmt.Sprintf("%s: %s\n", ip, status))
			}
			statusText.SetText(sb.String())
			nm.statusMu.RUnlock()

			refreshStreams()
		}
	}()

	mainTabs := container.NewAppTabs(
		container.NewTabItem("Config", container.NewVBox(
			widget.NewLabel("Network Adapter:"),
			adapterSelect,
			widget.NewLabel("Data Port:"),
			portEntry,
			widget.NewLabel("Target VPN IPs (one per line):"),
			container.NewScroll(targetsEntry),
			widget.NewSeparator(),
			widget.NewLabel("Active Streams:"),
			addStreamBtn,
			streamListContainer,
		)),
		container.NewTabItem("Dashboard", container.NewVBox(
			container.NewHBox(startBtn, stopBtn),
			widget.NewSeparator(),
			packetsLabel,
			bytesLabel,
			widget.NewLabel("Target Status:"),
			statusText,
		)),
	)

	w.SetContent(mainTabs)
	w.Resize(fyne.NewSize(600, 500))
	w.ShowAndRun()
}
