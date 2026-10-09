package video

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

func annexBNALs(nalus ...[]byte) []byte {
	var au []byte
	for _, n := range nalus {
		au = append(au, annexBStartCode...)
		au = append(au, n...)
	}
	return au
}

// Fixtures sintéticas: comprovam compatibilidade com rtp/h264.go do meowcaller,
// sem atribuir estes bytes à chamada real (que ainda não tem captura de payload).
func TestReceiveReferenceH264Formats(t *testing.T) {
	sps, pps := []byte{0x67, 0x42, 0xe0, 0x1f}, []byte{0x68, 0xce, 6, 0xe2}
	idr := append([]byte{0x65}, bytes.Repeat([]byte{9}, 1800)...)
	au := annexBNALs(sps, pps, idr)
	packed := au[4:]
	fu := func(header byte, body []byte) []byte { return append([]byte{0x7c, header}, body...) }
	tests := []struct {
		name    string
		packets [][]byte
	}{
		{"single-NAL-packed-AU", [][]byte{packed}},
		{"single-NAL-separate", [][]byte{sps, pps, idr}},
		{"STAP-A", [][]byte{transport.PackageH264STAPA([][]byte{sps, pps}), idr}},
		{"STAP-A-zero-tail", [][]byte{append(transport.PackageH264STAPA([][]byte{sps, pps}), 0, 0, 0), idr}},
		{"FU-A", transport.PackageH264NALU(packed)},
		{"FU-A-S-and-E", [][]byte{fu(0xc7, packed[1:])}},
		{"FU-A-reserved-bit-ignored", [][]byte{fu(0xe7, packed[1:])}},
		{"FU-A-empty-fragments", [][]byte{fu(0x87, nil), fu(7, packed[1:]), fu(0x47, nil)}},
		{"FU-A-type-zero-AnnexB", [][]byte{fu(0xc0, au)}},
		{"FU-A-private-type-AnnexB", [][]byte{fu(0xde, au)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _, peer := testPipeline(t)
			now := time.Unix(100, 0)
			manualVideoClock(p, &now)
			p.EnableEnvelope()
			var frames [][]byte
			p.OnFrame = func(frame []byte) { frames = append(frames, frame) }
			for i, payload := range tt.packets {
				receivePacket(t, p, peer, uint16(i+1), 90000, i == len(tt.packets)-1, payload, nil)
			}
			if len(frames) != 1 || !bytes.Equal(frames[0][12:], au) || frames[0][3] != 1 || binary.BigEndian.Uint32(frames[0][8:12]) != 90000 {
				t.Fatalf("frames=%x drops=%v; want one IDR envelope with AU=%x", frames, p.diag.rxDrops, au[:24])
			}
			if p.receive.needIDR || p.diag.rxIDRs != 1 || !bytes.Equal(p.receiveSPS, sps) || !bytes.Equal(p.receivePPS, pps) {
				t.Fatal("SPS/PPS/IDR não extraídos")
			}
		})
	}
}

func TestReceiveUnsupportedAndMalformedH264(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{"empty", nil}, {"type-zero", []byte{0, 0x65}},
		{"STAP-B", []byte{0x79, 0, 0, 0, 2, 0x65, 1}},
		{"MTAP16", []byte{0x7a, 0, 0, 0, 2, 0, 0, 0, 0x65, 1}},
		{"MTAP24", []byte{0x7b, 0, 0, 0, 2, 0, 0, 0, 0, 0x65, 1}},
		{"FU-B", []byte{0x7d, 0xc5, 0, 0, 1}},
		{"type-30", []byte{0x7e, 0x65, 1}}, {"type-31", []byte{0x7f, 0x65, 1}},
		{"raw-AnnexB", []byte{0, 0, 0, 1, 0x65, 1}},
		{"STAP-A-truncated", []byte{0x78, 0, 2, 0x65}},
		{"STAP-A-zero-size", []byte{0x78, 0, 0}},
		{"STAP-A-tail-after-IDR", []byte{0x78, 0, 2, 0x65, 1, 1}},
		{"FU-A-header-truncated", []byte{0x7c}},
		{"FU-A-orphan", []byte{0x7c, 0x45, 1}},
		{"FU-A-unfinished-at-marker", []byte{0x7c, 0x85, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _, peer := testPipeline(t)
			now := time.Unix(100, 0)
			manualVideoClock(p, &now)
			frames := 0
			p.OnFrame = func([]byte) { frames++ }
			receivePacket(t, p, peer, 1, 1, true, tt.payload, nil)
			if frames != 0 || !p.receive.needIDR || p.diag.rxIDRs != 0 || len(p.receiveSPS) != 0 || len(p.receivePPS) != 0 {
				t.Fatal("payload inválido classificou/entregou IDR")
			}
			receivePacket(t, p, peer, 2, 2, true, []byte{0x65, 1}, nil)
			if frames != 1 || p.receive.needIDR {
				t.Fatal("não recuperou depois de payload inválido")
			}
		})
	}
}

func TestReceiveVideoRTPPadding(t *testing.T) {
	for _, padding := range [][]byte{{0, 0, 3}, {0}, {10}} {
		p, _, peer := testPipeline(t)
		now := time.Unix(100, 0)
		manualVideoClock(p, &now)
		var frames [][]byte
		p.OnFrame = func(au []byte) { frames = append(frames, au) }
		h := media.NewRtpHeader(core.PayloadTypeWhatsAppH264, 1, 1, 42)
		h.Marker, h.Padding = true, true
		payload := append([]byte{0x78, 0, 2, 0x65, 1}, padding...)
		wire, err := peer.Protect(&media.RtpPacket{Header: h, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		p.HandleRelayData(wire)
		if padding[len(padding)-1] == 3 {
			if len(frames) != 1 || !bytes.Equal(frames[0], annexBNALs([]byte{0x65, 1})) {
				t.Fatal("padding RTP tratado como H264")
			}
		} else if len(frames) != 0 {
			t.Fatal("padding inválido aceito")
		}
	}
}

func TestCapturedRepeatsParameterSetsBeforeIDR(t *testing.T) {
	p, r, peer := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	sps, pps, idr := []byte{0x67, 0x42, 0xe0, 0x1f}, []byte{0x68, 1}, []byte{0x65, 2}
	p.FeedCaptured(annexBNALs(sps, pps, idr))
	now = now.Add(time.Second)
	p.FeedCaptured(annexBNALs(idr))
	now = now.Add(time.Second)
	newSPS, newPPS := []byte{0x67, 0x42, 0xe0, 0x20}, []byte{0x68, 3}
	p.FeedCaptured(annexBNALs(newSPS, newPPS, idr))
	now = now.Add(time.Second)
	p.FeedCaptured(annexBNALs(idr))
	for i, wire := range r.packets {
		pkt, err := peer.Unprotect(wire)
		if err != nil {
			t.Fatal(err)
		}
		want := [][]byte{sps, pps, idr}
		if i >= 2 {
			want = [][]byte{newSPS, newPPS, idr}
		}
		if got := transport.SplitAnnexB(append([]byte{0, 0, 0, 1}, pkt.Payload...)); !reflect.DeepEqual(got, want) {
			t.Fatalf("AU %d parameter sets=%x want=%x", i, got, want)
		}
	}
}

func TestCapturedPartialParameterSetsAndReset(t *testing.T) {
	p, r, peer := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	sps, pps, idr := []byte{0x67, 0x42, 0xe0, 0x1f}, []byte{0x68, 1}, []byte{0x65, 2}
	p.FeedCaptured(annexBNALs(sps, pps, idr))
	for _, partial := range [][]byte{annexBNALs(sps, idr), annexBNALs(pps, idr)} {
		now = now.Add(time.Second)
		p.FeedCaptured(partial)
	}
	for _, wire := range r.packets {
		pkt, err := peer.Unprotect(wire)
		if err != nil {
			t.Fatal(err)
		}
		if got := transport.SplitAnnexB(append([]byte{0, 0, 0, 1}, pkt.Payload...)); !reflect.DeepEqual(got, [][]byte{sps, pps, idr}) {
			t.Fatalf("parameter-set order=%x", got)
		}
	}
	p.Reset()
	if len(p.sendSPS) != 0 || len(p.sendPPS) != 0 {
		t.Fatal("parameter sets vazaram entre chamadas")
	}
}

func TestReceiveType18DoesNotApplyAudioDecapsulation(t *testing.T) {
	p, _, peer := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var frame []byte
	p.OnFrame = func(au []byte) { frame = au }
	// 0x92 tem nal_type=18 no H264. A referência preserva a NAL inteira;
	// não existe regra de prefixo MLow neste caminho.
	payload := []byte{0x92, 1, 0, 0, 0, 1, 0x65, 2}
	receivePacket(t, p, peer, 1, 1, true, payload, nil)
	if !bytes.Equal(frame, append([]byte{0, 0, 0, 1}, payload...)) {
		t.Fatalf("NAL tipo 18 foi desencapsulada: %x", frame)
	}
}

func TestCapturedClockSurvivesRTPRollover(t *testing.T) {
	p, r, peer := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	// Posição determinística perto do wrap, sem esperar 13 horas nem usar a chegada como clock.
	initial := p.rtp.CreatePacketWithDuration(nil, 0, false).Header.Timestamp
	p.rtp.AdvanceTimestamp(^uint32(0) - 3000 - initial)
	idr := annexBNALs([]byte{0x65, 1})
	p.FeedCaptured(idr)
	now = now.Add(50000 * time.Second)
	p.FeedCaptured(idr)
	now = now.Add(100 * time.Millisecond)
	p.FeedCaptured(idr)
	var timestamps []uint32
	for _, wire := range r.packets {
		pkt, err := peer.Unprotect(wire)
		if err != nil {
			t.Fatal(err)
		}
		timestamps = append(timestamps, pkt.Header.Timestamp)
	}
	if timestamps[0] != ^uint32(0)-3000 || timestamps[1] != 2999 || timestamps[2]-timestamps[1] != 6000 || p.clockTicks != 12000 {
		t.Fatal(timestamps, p.clockTicks)
	}
}

func TestCapturedH264SamplesExposeBitstreamProfile(t *testing.T) {
	p, _, _ := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var buf bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&buf, nil))
	idr := annexBNALs([]byte{0x67, 0x42, 0xe0, 0x1f}, []byte{0x68, 1}, []byte{0x65, 2})
	for i := 0; i < 20; i++ {
		p.FeedCaptured(idr)
		now = now.Add(100 * time.Millisecond)
	}
	rows := infoRecords(t, &buf, "video transmit H264 sample")
	if len(rows) != 8 {
		t.Fatal("amostras de IDR sem limite")
	}
	for _, row := range rows {
		if row["profile_idc"] != float64(66) || row["profile_iop"] != float64(224) || row["level_idc"] != float64(31) || row["sps_bytes"] != float64(4) || row["pps_bytes"] != float64(2) || row["packets"] != float64(1) {
			t.Fatal(row)
		}
	}
}

func TestCapturedTimestampIgnoresArrivalJitterAndBursts(t *testing.T) {
	p, r, peer := testPipeline(t)
	start := time.Unix(100, 0)
	now := start
	manualVideoClock(p, &now)
	p.rtp = media.NewH264Session(p.selfSsrc)
	idr := annexBNALs([]byte{0x65, 1})
	p.FeedCapturedAt(idr, start)
	now = start.Add(100 * time.Millisecond)
	p.FeedCapturedAt(idr, start.Add(50*time.Millisecond))
	p.FeedCapturedAt(idr, start.Add(50*time.Millisecond))
	p.FeedCapturedAt(idr, start.Add(49*time.Millisecond))
	now = start.Add(150 * time.Millisecond)
	p.FeedCapturedAt(idr, start.Add(120*time.Millisecond))
	var timestamps []uint32
	for _, wire := range r.packets {
		pkt, err := peer.Unprotect(wire)
		if err != nil {
			t.Fatal(err)
		}
		timestamps = append(timestamps, pkt.Header.Timestamp)
	}
	if len(timestamps) != 5 {
		t.Fatal(timestamps)
	}
	for i, step := range []uint32{0, 6000, 12000, 18000, 24000} {
		if got := timestamps[i] - timestamps[0]; got != step {
			t.Fatalf("AU %d timestamp step=%d want=%d", i, got, step)
		}
	}
}

func TestH264PayloadSamplesBoundedPerCall(t *testing.T) {
	p, _, peer := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var buf bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&buf, nil))
	accepted := append([]byte{0x65}, bytes.Repeat([]byte{9}, 30)...)
	for i := 0; i < 30; i++ {
		receivePacket(t, p, peer, uint16(i*2+1), uint32(i*2+1), true, accepted, nil)
		receivePacket(t, p, peer, uint16(i*2+2), uint32(i*2+2), true, []byte{0x7d, 0xc5, 0, 0, 1}, nil)
	}
	rows := infoRecords(t, &buf, "video receive H264 payload sample")
	accepts, rejects := 0, 0
	for _, row := range rows {
		if row["result"] == "accepted" {
			accepts++
			if row["payload_hex"] != hex.EncodeToString(accepted[:24]) {
				t.Fatal(row)
			}
		} else {
			rejects++
		}
		for _, key := range []string{"call_id", "pt", "marker", "seq", "ts", "ssrc", "payload_bytes", "payload_hex", "nal_type", "fu_active_before", "reason"} {
			if _, ok := row[key]; !ok {
				t.Fatalf("missing %s: %v", key, row)
			}
		}
		if row["level"] != "INFO" || row["pt"] != float64(97) || row["ssrc"] != float64(42) {
			t.Fatal(row)
		}
	}
	if accepts != 6 || rejects != 16 {
		t.Fatalf("accepted=%d rejected=%d", accepts, rejects)
	}
	// Rekey/Setup da mesma chamada não recarrega o orçamento.
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := p.Setup("CID", "999:1@lid", "111:1@lid", km, km); err != nil {
		t.Fatal(err)
	}
	receivePacket(t, p, peer, 61, 61, true, accepted, nil)
	if len(infoRecords(t, &buf, "video receive H264 payload sample")) != len(rows) {
		t.Fatal("rekey recarregou amostras")
	}
	p.BeginCall("CID2")
	if err := p.Setup("CID2", "999:1@lid", "111:1@lid", km, km); err != nil {
		t.Fatal(err)
	}
	receivePacket(t, p, peer, 62, 62, true, accepted, nil)
	rows = infoRecords(t, &buf, "video receive H264 payload sample")
	if len(rows) != 23 || rows[22]["call_id"] != "CID2" {
		t.Fatal("nova chamada não recarregou amostras")
	}
}

// Reproduz atrasos de IDR e rajadas do DataChannel: a packetização não altera o período.
func TestCapturedFixedCadenceAcrossFragmentedAUs(t *testing.T) {
	p, r, peer := testPipeline(t)
	start := time.Unix(100, 0)
	now := start
	manualVideoClock(p, &now)
	idr := annexBNALs([]byte{0x67, 0x42, 0xc0, 22}, []byte{0x68, 1}, append([]byte{0x65}, bytes.Repeat([]byte{7}, 4200)...))
	delta := annexBNALs(append([]byte{0x41}, bytes.Repeat([]byte{8}, 1400)...))
	var firstTS uint32
	var previousSeq uint16
	for frame, ms := range []int{0, 49, 117, 183, 251, 382, 382, 381, 1700} {
		now = start.Add(time.Duration(ms) * time.Millisecond)
		au := delta
		if frame == 0 || frame == 5 || frame == 8 {
			au = idr
		}
		before := len(r.packets)
		p.FeedCapturedAt(au, now)
		wires := r.packets[before:]
		if len(wires) < 2 {
			t.Fatalf("AU %d não foi fragmentada: %d", frame, len(wires))
		}
		var depack transport.H264Depacketizer
		var rebuilt []byte
		for fragment, wire := range wires {
			pkt, err := peer.Unprotect(wire)
			if err != nil {
				t.Fatal(err)
			}
			if frame == 0 && fragment == 0 {
				firstTS = pkt.Header.Timestamp
			} else if pkt.Header.SequenceNumber != previousSeq+1 {
				t.Fatal("sequência RTP não avançou")
			}
			previousSeq = pkt.Header.SequenceNumber
			if pkt.Header.Timestamp-firstTS != uint32(frame)*6000 || pkt.Header.Marker != (fragment == len(wires)-1) {
				t.Fatalf("AU %d fragmento %d: timestamp=%d marker=%v", frame, fragment, pkt.Header.Timestamp-firstTS, pkt.Header.Marker)
			}
			for _, nalu := range depack.Depacketize(pkt.Payload) {
				rebuilt = append(rebuilt, annexBStartCode...)
				rebuilt = append(rebuilt, nalu...)
			}
		}
		if !bytes.Equal(rebuilt, au) {
			t.Fatalf("AU %d mudou na fragmentação", frame)
		}
	}
}

func TestCapturedNominalClockBeyondTwoRTPWraps(t *testing.T) {
	p, _, _ := testPipeline(t)
	start := time.Unix(100, 0)
	now := start
	manualVideoClock(p, &now)
	idr := annexBNALs([]byte{0x65, 1})
	p.FeedCapturedAt(idr, now)
	// Simula o contador de uma chamada >27 horas sem gerar 1,5 milhão de AUs.
	const ticks uint64 = 100000 * 90000
	p.clockTicks = ticks
	p.rtp.AdvanceTimestamp(uint32(ticks & 0xffffffff))
	now = start.Add(100000 * time.Second)
	p.FeedCapturedAt(idr, now)
	wantAt := start.Add(100000*time.Second + time.Second/15)
	if p.clockTicks != ticks+6000 || !p.diag.srRtpAt.Equal(wantAt) {
		t.Fatalf("clock nominal/SR após dois wraps: ticks=%d at=%v want=%v", p.clockTicks, p.diag.srRtpAt, wantAt)
	}
}
