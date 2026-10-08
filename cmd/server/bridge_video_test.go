package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/pion/webrtc/v4"
)

func TestVideoBrowserDiagnosticsExposeMissingChannelAndSendFailure(t *testing.T) {
	var buf bytes.Buffer
	b := &Bridge{log: slog.New(slog.NewJSONHandler(&buf, nil)), callID: "video-test"}
	if err := b.WriteVideo([]byte{1}); err != nil {
		t.Fatal(err)
	}
	b.RequestVideoKeyframe()
	b.LogVideoDiagnostics()
	var row map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &row); err != nil {
		t.Fatal(err)
	}
	if row["dc_state"] != "missing" || row["no_channel"] != float64(1) || row["sent_aus"] != float64(0) || row["kf_errors"] != float64(1) {
		t.Fatal(row)
	}
	// Canal ainda não aberto: Send falha; não contar como AU entregue.
	b.videoDC.Store(&webrtc.DataChannel{})
	if err := b.WriteVideo([]byte{1}); err == nil {
		t.Fatal("canal fechado parece sucesso")
	}
	b.handleVideoMessage(webrtc.DataChannelMessage{IsString: true, Data: []byte("B1")})
	b.SetVideoBitrate(300000)
	if b.videoStats.sent.Load() != 0 || b.videoStats.sendErrors.Load() != 1 || b.videoStats.bitrateErrors.Load() != 1 {
		t.Fatal("erros de Send ocultos")
	}
}
