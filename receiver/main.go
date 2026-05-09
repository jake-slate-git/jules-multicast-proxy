package main

import (
	"fmt"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

func main() {
	a := app.NewWithID("com.jules.multicast.receiver")
	w := a.NewWindow("Multicast Stream Receiver")

	rm := NewReceiverManager()

	// UI
	senderEntry := widget.NewEntry()
	senderEntry.SetPlaceHolder("Sender VPN IP (e.g. 10.8.0.1)")
	senderEntry.OnChanged = func(s string) {
		rm.mu.Lock()
		rm.SenderIP = s
		rm.mu.Unlock()
	}

	startBtn := widget.NewButton("START", func() {
		rm.Start()
	})
	stopBtn := widget.NewButton("STOP", func() {
		rm.Stop()
	})

	statusLabel := widget.NewLabel("")

	go func() {
		for {
			time.Sleep(time.Second)
			rm.mu.RLock()
			senderIP := rm.SenderIP
			// Sync entry with discovered IP
			if senderEntry.Text == "" && senderIP != "" {
				senderEntry.SetText(senderIP)
			}

			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("Status: %s\n", func() string {
				if rm.IsRunning { return "Running" }
				return "Stopped"
			}()))
			sb.WriteString(fmt.Sprintf("Sender: %s\n", senderIP))
			sb.WriteString(fmt.Sprintf("Listening on UDP Port: %d\n", rm.DataPort))
			sb.WriteString("------------------------------------------\n")
			for _, state := range rm.ActiveStreams {
				sb.WriteString(fmt.Sprintf("Drone: %s\n", state.Config.DroneName))
				sb.WriteString(fmt.Sprintf("  Multicast: %s:%d\n", state.Config.SourceMulticastIP, state.Config.SourceMulticastPort))
				sb.WriteString(fmt.Sprintf("  Last Heartbeat: %s\n", state.LastSeen.Format("15:04:05")))
				sb.WriteString(fmt.Sprintf("  Traffic: %d pkts (%d KB)\n", state.Packets, state.Bytes/1024))
				sb.WriteString("\n")
			}
			rm.mu.RUnlock()
			statusLabel.SetText(sb.String())
		}
	}()

	w.SetContent(container.NewVBox(
		widget.NewLabelWithStyle("Receiver Status Dashboard", fyne.TextAlignCenter, fyne.TextStyle{Bold: true}),
		widget.NewSeparator(),
		widget.NewLabel("Sender IP:"),
		senderEntry,
		container.NewHBox(startBtn, stopBtn),
		widget.NewSeparator(),
		container.NewVScroll(statusLabel),
	))

	w.Resize(fyne.NewSize(500, 400))
	w.ShowAndRun()
}
