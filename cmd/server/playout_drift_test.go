package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"wacalls/internal/voip/core"
)

type driftTestHandler struct {
	slog.Handler
	logged chan struct{}
}

func (h *driftTestHandler) Handle(ctx context.Context, r slog.Record) error {
	err := h.Handler.Handle(ctx, r)
	if r.Message == "[voz-drift]" {
		select {
		case h.logged <- struct{}{}:
		default:
		}
	}
	return err
}

func TestPlayoutDriftReportsOneLineWithWindowDeltas(t *testing.T) {
	sess, ac := ringingTimeoutSession()
	defer ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
	defer sess.removeCall("c1")
	var buf bytes.Buffer
	handler := &driftTestHandler{Handler: slog.NewJSONHandler(&buf, nil), logged: make(chan struct{}, 1)}
	sess.log = slog.New(handler)
	ac.playout.push(blocoConst(3000*48, 0.5))
	ac.stopPlayout, ac.playoutDone = make(chan struct{}), make(chan struct{})
	sess.startBrowserPlayout("c1", ac)
	select {
	case <-handler.logged:
	case <-time.After(10 * time.Second):
		stopBrowserPlayout(ac)
		t.Fatal("log de 5s não emitido")
	}
	stopBrowserPlayout(ac) // buffer só é lido depois que o writer termina
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte{'\n'})
	if len(lines) != 1 {
		t.Fatalf("linhas por chamada/período=%d", len(lines))
	}
	var record map[string]any
	if err := json.Unmarshal(lines[0], &record); err != nil {
		t.Fatal(err)
	}
	if record["call_id"] != "c1" || record["cap_drop_ms_delta"] != float64(1000) || record["hard_drop_delta"] != float64(48000) || record["padding_ms_delta"] != float64(0) || record["reordered_delta"] != float64(0) || record["dup_delta"] != float64(0) {
		t.Fatalf("contadores do período=%v", record)
	}
	drain, ok := record["drain_frames_delta"].(float64)
	if !ok || drain <= 0 || drain != float64(ac.playout.trimmed/playoutFrame48) {
		t.Fatalf("drenagem=%v", record["drain_frames_delta"])
	}
	// Entrada é taxa por segundo, enquanto hard_drop é o acumulado da janela.
	inRate, ok := record["in_smp_s"].(float64)
	if !ok || inRate <= 0 || inRate >= float64(ac.playout.in) {
		t.Fatalf("taxa de entrada=%v", record["in_smp_s"])
	}
}
