package video

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

func infoRecords(t *testing.T, buf *bytes.Buffer, name string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record["msg"] == name {
			out = append(out, record)
		}
	}
	return out
}

func TestVideoDiagnosticsStartWithoutAnyMedia(t *testing.T) {
	var buf bytes.Buffer
	now := time.Unix(100, 0)
	r := &fakeRelay{}
	p := New(slog.New(slog.NewJSONHandler(&buf, nil)), r)
	t.Cleanup(p.Reset)
	p.now = func() time.Time { return now }
	var delay time.Duration
	p.afterFunc = func(d time.Duration, _ func()) *time.Timer {
		delay = d
		return time.NewTimer(24 * time.Hour)
	}
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := p.Setup("silent-call", "999:1@lid", "111:1@lid", km, km); err != nil {
		t.Fatal(err)
	}
	if p.monitorTimer == nil || delay != videoDiagnosticInterval {
		t.Fatal("monitor não iniciou no Setup sem mídia")
	}
	for i := 0; i < 2; i++ {
		now = now.Add(videoDiagnosticInterval)
		p.monitorVideo(p.monitorEpoch)
	}
	rows := infoRecords(t, &buf, "video diagnostics")
	if len(rows) != 2 || rows[1]["rx_packets"] != float64(0) || rows[1]["rx_last_packet_ms"] != float64(-1) || rows[1]["need_idr"] != true {
		t.Fatal(rows)
	}
}

func TestVideoDiagnosticsContinueWhileFrameCallbackBlocked(t *testing.T) {
	p, _, peer := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var buf bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&buf, nil))
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	p.OnFrame = func([]byte) { close(entered); <-release }
	// Simula um WriteVideo bloqueado; o monitor não pode depender de receiveMu.
	packet := protectVideoPacket(t, peer, 1, 1, true, []byte{0x65, 1})
	go func() { defer close(done); p.HandleRelayData(packet) }()
	<-entered
	now = now.Add(videoDiagnosticInterval)
	monitored := make(chan struct{})
	go func() { defer close(monitored); p.monitorVideo(p.monitorEpoch) }()
	select {
	case <-monitored:
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("callback de frame bloqueou diagnósticos")
	}
	close(release)
	<-done
	rows := infoRecords(t, &buf, "video diagnostics")
	if len(rows) != 1 || rows[0]["rx_complete"] != float64(1) || rows[0]["rx_forwarded"] != float64(1) {
		t.Fatal(rows)
	}
}

func TestVideoINFOCountersContinueDuringSilenceAndReset(t *testing.T) {
	p, _, peer := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var buf bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&buf, nil)) // INFO, igual à produção
	p.OnFrame = func([]byte) {}
	callbacks := 0
	p.OnDiagnostics = func(id string) {
		p.mu.Lock()
		p.mu.Unlock() // callback fora de mu
		if id != "CID" {
			t.Fatal(id)
		}
		callbacks++
	}
	receivePacket(t, p, peer, 1, 1, true, []byte{0x65, 1}, nil)
	receivePacket(t, p, peer, 2, 2, true, []byte{0x7c, 0x41, 1}, nil) // continuação órfã
	receivePacket(t, p, peer, 3, 3, true, []byte{0x41, 1}, nil)       // delta descartada
	p.FeedCaptured([]byte{1, 2})
	if records := infoRecords(t, &buf, "video first PLI"); len(records) != 1 || records[0]["relay_sends"] != float64(1) || records[0]["compound_hex"] == "" {
		t.Fatalf("primeiro PLI não visível em INFO: %v", records)
	}
	now = now.Add(5*time.Second - time.Nanosecond)
	p.monitorVideo(p.monitorEpoch)
	if len(infoRecords(t, &buf, "video diagnostics")) != 0 {
		t.Fatal("log antes de 5s")
	}
	now = now.Add(time.Nanosecond)
	p.monitorVideo(p.monitorEpoch)
	rows := infoRecords(t, &buf, "video diagnostics")
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	r := rows[0]
	for key, want := range map[string]any{"call_id": "CID", "need_idr": true, "rx_packets": float64(3), "rx_aus": float64(3), "rx_complete": float64(2), "rx_forwarded": float64(1), "tx_aus": float64(1), "tx_submitted": float64(0)} {
		if r[key] != want {
			t.Fatalf("%s=%v want=%v", key, r[key], want)
		}
	}
	drops := r["rx_drops"].(map[string]any)
	if drops["invalid-h264-payload"] != float64(1) || drops["waiting-idr"] != float64(1) {
		t.Fatal(drops)
	}
	now = now.Add(5 * time.Second)
	p.monitorVideo(p.monitorEpoch)
	rows = infoRecords(t, &buf, "video diagnostics")
	if callbacks != 2 || len(rows) != 2 || rows[1]["rx_last_packet_ms"] != float64(10000) {
		t.Fatal(rows)
	}
	staleEpoch := p.monitorEpoch
	p.Reset()
	p.monitorVideo(staleEpoch)
	if callbacks != 2 || p.monitorTimer != nil || p.diag.rxPackets != 0 {
		t.Fatal("monitor/contadores sobreviveram ao reset")
	}
}

func TestIDRWaitStartsWithRTPAfterEarlyBrowserKF(t *testing.T) {
	p, _, peer := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	// manualVideoClock prepara esperas antigas em outros testes; aqui conserva
	// o estado real de Setup, antes de qualquer RTP.
	p.receive.idrWaitAt = time.Time{}
	p.RequestPeerKeyframe()
	if !p.receive.idrWaitAt.IsZero() || p.peerPLICount != 0 {
		t.Fatal("KF sem SSRC iniciou espera/envio")
	}
	now = now.Add(time.Minute)
	receivePacket(t, p, peer, 1, 1, true, []byte{0x41, 1}, nil)
	if p.receive.idrWaitAt != now || p.receive.idrWaitExpired || p.diag.rxDrops["waiting-idr"] != 1 || p.peerPLICount != 1 {
		t.Fatal("fase de áudio contada como espera por IDR")
	}
}

func TestVideoPeriodicSenderReportWithoutNewCaptures(t *testing.T) {
	p, r, _ := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var deadlines []time.Duration
	p.afterFunc = func(delay time.Duration, _ func()) *time.Timer {
		deadlines = append(deadlines, delay)
		return time.NewTimer(24 * time.Hour)
	}
	p.FeedCaptured([]byte{0, 0, 0, 1, 0x65, 1})
	if deadlines[len(deadlines)-1] != videoReportInterval {
		t.Fatal(deadlines)
	}
	rtpHeader, err := media.DecodeRtpHeader(r.packets[0])
	if err != nil {
		t.Fatal(err)
	}
	keys := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	peer, err := media.NewSrtcpSession(keys, keys)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		now = time.Unix(100, 0).Add(time.Duration(i) * videoReportInterval)
		p.monitorVideo(p.monitorEpoch)
		plain, err := peer.Unprotect(r.packets[len(r.packets)-1])
		if err != nil {
			t.Fatal(err)
		}
		if len(plain) != 60 || plain[1] != 200 || plain[29] != 202 || binary.BigEndian.Uint32(plain[20:24]) != 1 || binary.BigEndian.Uint32(plain[24:28]) != 2 {
			t.Fatalf("SR sem RTP novo: %x", plain)
		}
		wantTS := rtpHeader.Timestamp + uint32(i)*135000
		if got := binary.BigEndian.Uint32(plain[16:20]); got != wantTS {
			t.Fatalf("clock SR=%d want=%d", got, wantTS)
		}
	}
	stale := p.monitorEpoch
	p.Reset()
	before := r.broadcasts
	p.monitorVideo(stale)
	if r.broadcasts != before {
		t.Fatal("SR enviado depois do reset")
	}
}

type senderStatsRelay struct {
	*fakeRelay
	p               *Pipeline
	packets, octets uint32
}

func (r *senderStatsRelay) VideoSenderStats(ssrc uint32) (uint32, uint32) {
	r.p.mu.Lock()
	defer r.p.mu.Unlock() // snapshot nunca adquirido sob p.mu
	return r.packets, r.octets
}

func TestVideoSRUsesActualTransportCounters(t *testing.T) {
	p, base, _ := testPipeline(t)
	r := &senderStatsRelay{fakeRelay: base, p: p}
	p.relay = r
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	p.FeedCaptured(append([]byte{0, 0, 0, 1, 0x65}, bytes.Repeat([]byte{7}, 1800)...))
	before := r.broadcasts
	now = now.Add(videoReportInterval)
	p.monitorVideo(p.monitorEpoch)
	if r.broadcasts != before {
		t.Fatal("SR antes de RTP sair pelo transporte")
	}
	r.packets, r.octets = 1, 800 // só um dos três fragmentos foi enviado
	now = now.Add(videoReportInterval)
	p.monitorVideo(p.monitorEpoch)
	keys := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	peer, err := media.NewSrtcpSession(keys, keys)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := peer.Unprotect(r.packetsDataLast())
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(plain[20:24]) != 1 || binary.BigEndian.Uint32(plain[24:28]) != 800 {
		t.Fatalf("SR anunciou fragmentos só enfileirados: %x", plain)
	}
}

func (r *senderStatsRelay) packetsDataLast() []byte {
	return r.fakeRelay.packets[len(r.fakeRelay.packets)-1]
}

func TestAuthenticatedREMBEffectsOnlyAfterWholeCompoundValidation(t *testing.T) {
	p, _, _ := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	keys := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	peer, err := media.NewSrtcpSession(keys, keys)
	if err != nil {
		t.Fatal(err)
	}
	var targets []uint64
	p.OnBitrateRequired = func(bps uint64) { p.mu.Lock(); p.mu.Unlock(); targets = append(targets, bps) }
	remb, _ := hex.DecodeString("9fce00050000002a0000000052454d42010adc6c55667788")
	send := func(b []byte) {
		encrypted, err := peer.Protect(b)
		if err != nil {
			t.Fatal(err)
		}
		p.HandleRTCP(encrypted)
	}
	send(remb) // outro SSRC
	if len(targets) != 0 || !p.diag.rembSeen || p.diag.rembVideo {
		t.Fatal("REMB de outro stream mudou encoder")
	}
	binary.BigEndian.PutUint32(remb[20:24], p.selfSsrc)
	send(append(bytes.Clone(remb), 0x80, 206, 0, 10))
	if len(targets) != 0 {
		t.Fatal("compound malformado mudou encoder")
	}
	encrypted, err := peer.Protect(remb)
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(encrypted)
	bad[len(bad)-1] ^= 1
	p.HandleRTCP(bad)
	if len(targets) != 0 {
		t.Fatal("REMB adulterado mudou encoder")
	}
	p.HandleRTCP(encrypted)
	p.HandleRTCP(encrypted) // replay de outro relay
	if len(targets) != 1 || targets[0] != 750000 || p.diag.rembVideoBitrate != 750000 {
		t.Fatal(targets)
	}
	clear(remb[17:20])
	send(remb)
	if len(targets) != 2 || targets[1] != 0 {
		t.Fatal("REMB zero ignorado")
	}
	remb[17], remb[18], remb[19] = 0x0a, 0xdc, 0x6c
	multi := append(bytes.Clone(remb), 0, 0, 0, 0)
	multi[3], multi[16] = 6, 2
	binary.BigEndian.PutUint32(multi[24:28], p.selfAudioSsrc)
	send(multi)
	if targets[len(targets)-1] != 750000-uint64(media.DefaultCodecOptions.Bitrate) {
		t.Fatal("REMB compartilhado não reservou áudio")
	}
	clear(multi[17:20])
	multi[19] = 1
	send(multi)
	if targets[len(targets)-1] != 0 {
		t.Fatal("subtração de áudio causou underflow")
	}
}
