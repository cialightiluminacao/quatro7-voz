package video

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"sync"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

type fakeRelay struct {
	mu         sync.Mutex
	broadcasts int
	connected  bool
	buffered   uint64
	packets    [][]byte
	growBuffer bool
}

func (f *fakeRelay) Broadcast(data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.broadcasts++
	f.packets = append(f.packets, bytes.Clone(data))
	if f.growBuffer {
		f.buffered += uint64(len(data))
	}
}
func (f *fakeRelay) BufferedAmount() uint64             { f.mu.Lock(); defer f.mu.Unlock(); return f.buffered }
func (f *fakeRelay) HasConnection() bool                { f.mu.Lock(); defer f.mu.Unlock(); return f.connected }
func (f *fakeRelay) SetStreamSsrcs(self, peer []uint32) {}

func (f *fakeRelay) setBuffered(n uint64) { f.mu.Lock(); f.buffered = n; f.mu.Unlock() }

func testPipeline(t *testing.T) (*Pipeline, *fakeRelay, *media.SrtpSession) {
	t.Helper()
	r := &fakeRelay{connected: true}
	p := New(nil, r)
	t.Cleanup(p.Reset)
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := p.Setup("CID", "999:1@lid", "111:1@lid", km, km); err != nil {
		t.Fatal(err)
	}
	peer, err := media.NewSrtpSession(km, km, core.SRTPRecvAuthTagLen, core.SRTPSendAuthTagLen)
	if err != nil {
		t.Fatal(err)
	}
	return p, r, peer
}

// Testes que avançam o relógio manualmente também disparam flushReceive à mão.
// Um timer real de 1 ns concorria com esse relógio e com as leituras do fakeRelay.
// Os testes de timer/guard continuam usando time.AfterFunc real.
func manualVideoClock(p *Pipeline, now *time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopReceiveTimerLocked()
	p.stopMonitorLocked()
	p.now = func() time.Time { return *now }
	p.afterFunc = func(time.Duration, func()) *time.Timer { return time.NewTimer(24 * time.Hour) }
	p.diag.lastLog = time.Time{}
	p.startMonitorLocked()
	if p.receive.needIDR {
		p.receive.idrWaitAt = *now
	}
}

func receivePacket(t *testing.T, p *Pipeline, peer *media.SrtpSession, seq uint16, ts uint32, marker bool, payload, ext []byte) {
	t.Helper()
	h := media.NewRtpHeader(core.PayloadTypeWhatsAppH264, seq, ts, 42)
	h.Marker = marker
	if ext != nil {
		h.Extension, h.ExtensionProfile, h.ExtensionData = true, videoExtProfile, ext
	}
	data, err := peer.Protect(&media.RtpPacket{Header: h, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	p.HandleRelayData(data)
}

func TestFeedCapturedNoopBeforeSetup(t *testing.T) {
	r := &fakeRelay{connected: true}
	p := New(nil, r)
	p.FeedCaptured([]byte{0, 0, 0, 1, 0x65, 0x01, 0x02})
	if r.broadcasts != 0 {
		t.Fatalf("expected no broadcast before Setup, got %d", r.broadcasts)
	}
}

func TestHandleRelayDataNoopBeforeSetup(t *testing.T) {
	r := &fakeRelay{connected: true}
	p := New(nil, r)
	emitted := false
	p.OnFrame = func([]byte) { emitted = true }
	p.HandleRelayData(make([]byte, 16))
	if emitted {
		t.Fatal("expected no frame emitted before Setup")
	}
}

func TestResetClearsState(t *testing.T) {
	p := New(nil, &fakeRelay{})
	p.depack = &transport.H264Depacketizer{}
	p.frameBuf = []byte{1, 2, 3}
	p.selfSsrc = 42
	p.envelopeCapable, p.rtpOrientationKnown, p.peerOrientation = true, true, 3
	p.receive = receiveState{seqKnown: true, needIDR: true, auSequence: 99}
	p.lastKFAt, p.firstAUAt = time.Now(), time.Now()
	p.Reset()
	if p.depack != nil || p.frameBuf != nil || p.selfSsrc != 0 || p.envelopeCapable || p.rtpOrientationKnown || p.peerOrientation != 0 || p.receive != (receiveState{}) || !p.lastKFAt.IsZero() || !p.firstAUAt.IsZero() {
		t.Fatal("Reset did not clear pipeline state")
	}
}

func TestReceiveOrientationAndNegotiatedEnvelope(t *testing.T) {
	p, _, peer := testPipeline(t)
	var frames [][]byte
	var orientations []int
	p.OnFrame = func(au []byte) { frames = append(frames, au) }
	p.OnOrientation = func(callID string, o int) {
		if callID != "CID" {
			t.Errorf("callID=%q", callID)
		}
		orientations = append(orientations, o)
	}
	if got := p.SetOrientationFallback(2); got != 2 {
		t.Fatal(got)
	}
	// Legado: exatamente o Annex-B de antes, mesmo com orientação conhecida.
	receivePacket(t, p, peer, 10, 0x10203040, true, []byte{0x65, 9}, nil)
	if !bytes.Equal(frames[0], []byte{0, 0, 0, 1, 0x65, 9}) {
		t.Fatalf("legado=%x", frames[0])
	}
	p.EnableEnvelope()
	p.EnableEnvelope() // negociação idempotente, sem resetar sequência
	receivePacket(t, p, peer, 11, 0x11223344, true, []byte{0x41, 8}, buildWhatsappVideoExt(videoMediaFrameDelta|3, nil, 0))
	want := []byte{0x51, 1, 3, 0, 0, 0, 0, 2, 0x11, 0x22, 0x33, 0x44, 0, 0, 0, 1, 0x41, 8}
	if !bytes.Equal(frames[1], want) {
		t.Fatalf("envelope=%x quero=%x", frames[1], want)
	}
	if got := p.SetOrientationFallback(1); got != 3 {
		t.Fatalf("stanza sobrepôs RTP: %d", got)
	}
	// Ausência/malformação preservam último RTP; SPS sozinho NÃO é IDR.
	receivePacket(t, p, peer, 12, 88, true, []byte{0x67, 1}, []byte{0x30, 1, 0xf0, 0})
	if frames[2][2] != 3 || frames[2][3] != 0 {
		t.Fatalf("SPS/orientação=%x", frames[2])
	}
	for o := 0; o < 4; o++ {
		receivePacket(t, p, peer, uint16(13+o), uint32(89+o), true, []byte{0x65, 2}, buildWhatsappVideoExt(videoMediaFrameIDR|byte(o), nil, 0))
		frame := frames[len(frames)-1]
		if frame[2] != byte(o) || frame[3] != 1 || binary.BigEndian.Uint32(frame[4:8]) != uint32(4+o) {
			t.Fatalf("o=%d envelope=%x", o, frame)
		}
	}
	if !reflect.DeepEqual(orientations, []int{3, 0, 1, 2, 3}) {
		t.Fatal(orientations)
	}
}

func TestEnvelopeAndOrientationBeforeSetupAndAcrossRekey(t *testing.T) {
	p := New(nil, &fakeRelay{connected: true})
	p.BeginCall("CID")
	p.EnableEnvelope()
	p.SetOrientationFallback(2)
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := p.Setup("CID", "999:1@lid", "111:1@lid", km, km); err != nil {
		t.Fatal(err)
	}
	if !p.envelopeCapable || p.peerOrientation != 2 {
		t.Fatal("Setup perdeu negociação/fallback")
	}
	p.receive.auSequence = 8
	p.rtpOrientationKnown, p.peerOrientation = true, 3
	if err := p.Setup("CID", "999:1@lid", "111:1@lid", km, km); err != nil {
		t.Fatal(err)
	}
	if !p.envelopeCapable || p.peerOrientation != 3 || p.receive.auSequence != 8 || !p.rtpOrientationKnown {
		t.Fatal("rekey perdeu metadados da chamada")
	}
	p.BeginCall("NOVA")
	if p.envelopeCapable || p.peerOrientation != 0 || p.rtpOrientationKnown || p.receive.auSequence != 0 {
		t.Fatal("metadados vazaram entre chamadas")
	}
}

func TestOrientationComesFromFirstFragmentOfAU(t *testing.T) {
	p, _, peer := testPipeline(t)
	p.EnableEnvelope()
	p.SetOrientationFallback(1)
	var frames [][]byte
	p.OnFrame = func(au []byte) { frames = append(frames, au) }
	idr := append([]byte{0x65}, bytes.Repeat([]byte{7}, 1800)...)
	fragments := transport.PackageH264NALU(idr)
	for i, fragment := range fragments {
		var ext []byte
		if i == 0 {
			ext = buildWhatsappVideoExt(videoMediaFrameIDR|3, nil, 0)
		}
		receivePacket(t, p, peer, uint16(i), 0xfffffff0, i == len(fragments)-1, fragment, ext)
	}
	if len(frames) != 1 || frames[0][2] != 3 || binary.BigEndian.Uint32(frames[0][8:12]) != 0xfffffff0 {
		t.Fatal("orientação/timestamp do primeiro fragmento perdidos")
	}
	// Sem CVO na AU seguinte, manter último conhecido, mesmo no rollover de ts.
	receivePacket(t, p, peer, 3, 10, true, []byte{0x41, 2}, nil)
	if len(frames) != 2 || frames[1][2] != 3 || binary.BigEndian.Uint32(frames[1][8:12]) != 10 {
		t.Fatal("rollover alterou a orientação/timestamp")
	}
}

func TestFUASequenceGapWaitsForIDR(t *testing.T) {
	for _, loss := range []string{"middle", "start", "end", "marker", "whole AU"} {
		t.Run(loss, func(t *testing.T) {
			p, _, peer := testPipeline(t)
			p.EnableEnvelope()
			var frames [][]byte
			p.OnFrame = func(au []byte) { frames = append(frames, au) }
			receivePacket(t, p, peer, 65530, 10, true, []byte{0x65, 1}, nil)
			frag := transport.PackageH264NALU(append([]byte{0x41}, bytes.Repeat([]byte{7}, 2000)...))
			for i, f := range frag {
				if (loss == "middle" && i == 1) || (loss == "start" && i == 0) || ((loss == "end" || loss == "marker") && i == 2) || loss == "whole AU" {
					continue
				}
				marker := i == len(frag)-1
				if loss == "marker" && i == 1 {
					marker = true
				} // marker com FU incompleto
				receivePacket(t, p, peer, uint16(65531+i), 20, marker, f, nil)
			}
			// Aguarda a janela de jitter antes de declarar a perda definitiva.
			p.mu.Lock()
			p.drainReceiveLocked(p.now().Add(videoReorderWait))
			p.mu.Unlock()
			receivePacket(t, p, peer, 65534, 30, true, []byte{0x41, 3}, nil)
			receivePacket(t, p, peer, 65535, 40, true, []byte{0x67, 4}, nil)
			p.mu.Lock()
			p.drainReceiveLocked(p.now().Add(videoReorderWait))
			p.mu.Unlock()
			if len(frames) != 1 {
				t.Fatalf("referências quebradas entregues: %d frames", len(frames))
			}
			// Só um IDR COMPLETO, inclusive fragmentado e cruzando rollover, libera.
			idr := append([]byte{0x65}, bytes.Repeat([]byte{9}, 1800)...)
			frag = transport.PackageH264NALU(idr)
			for i, f := range frag {
				receivePacket(t, p, peer, uint16(i), 50, i == len(frag)-1, f, nil)
			}
			receivePacket(t, p, peer, 3, 60, true, []byte{0x41, 5}, nil)
			if len(frames) != 3 || frames[1][3] != 1 || !bytes.Equal(frames[1][12:], append([]byte{0, 0, 0, 1, 0x67, 4, 0, 0, 0, 1}, idr...)) || frames[2][3] != 0 {
				t.Fatalf("recuperação IDR: %d frames", len(frames))
			}
			if binary.BigEndian.Uint32(frames[1][4:8]) <= 2 {
				t.Fatal("sequência não inclui drops")
			}
		})
	}
}

func TestReceiveMultiNALGapAndDuplicates(t *testing.T) {
	p, _, peer := testPipeline(t)
	var frames [][]byte
	p.OnFrame = func(au []byte) { frames = append(frames, au) }
	// SPS em um pacote seguido de FU-A IDR: perda não pode entregar nem o SPS.
	receivePacket(t, p, peer, 100, 1, false, []byte{0x67, 1}, nil)
	receivePacket(t, p, peer, 100, 1, false, []byte{0x67, 1}, nil) // duplicado
	receivePacket(t, p, peer, 101, 1, false, []byte{0x7c, 0x85, 1}, nil)
	receivePacket(t, p, peer, 103, 1, true, []byte{0x7c, 0x45, 3}, nil)
	receivePacket(t, p, peer, 102, 1, false, []byte{0x7c, 5, 2}, nil) // atrasado
	receivePacket(t, p, peer, 104, 2, true, []byte{0x41, 4}, nil)
	if len(frames) != 2 || !bytes.Equal(frames[0], []byte{0, 0, 0, 1, 0x67, 1, 0, 0, 0, 1, 0x65, 1, 2, 3}) || p.receive.needIDR {
		t.Fatal("reordenação perdeu AU completa")
	}
	// Formato WhatsApp: SPS/PPS/IDR juntos, fragmentados como uma NAL.
	nalu := append([]byte{0x67, 1, 0, 0, 0, 1, 0x68, 2, 0, 0, 0, 1, 0x65}, bytes.Repeat([]byte{9}, 1800)...)
	frag := transport.PackageH264NALU(nalu)
	for i, f := range frag {
		receivePacket(t, p, peer, uint16(105+i), 3, i == len(frag)-1, f, nil)
	}
	if len(frames) != 3 || !bytes.Equal(frames[2], append([]byte{0, 0, 0, 1}, nalu...)) {
		t.Fatal("IDR dentro de AU empacotada não recuperou")
	}
}

func TestRelayDropRequestsKeyframeWithRateLimit(t *testing.T) {
	p, r, _ := testPipeline(t)
	p.EnableEnvelope()
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var requests []time.Time
	p.OnKeyframeRequired = func() { requests = append(requests, now); p.EnableEnvelope() } // callback sem mu
	idr, delta := []byte{0, 0, 0, 1, 0x65, 1}, []byte{0, 0, 0, 1, 0x41, 2}
	p.FeedCaptured(idr)
	r.setBuffered(congestionDropBytes)
	p.FeedCaptured(delta) // limite inclusivo ainda permite a AU
	if r.broadcasts != 2 {
		t.Fatal(r.broadcasts)
	}
	r.setBuffered(congestionDropBytes + 1)
	p.FeedCaptured(delta)
	if r.broadcasts != 2 || len(requests) != 1 {
		t.Fatalf("drop=%d KF=%d", r.broadcasts, len(requests))
	}
	now = now.Add(999 * time.Millisecond)
	p.FeedCaptured(idr) // IDR sob congestão também descartado, sem liberar referências
	r.setBuffered(0)
	p.FeedCaptured(delta)
	if len(requests) != 1 || r.broadcasts != 2 {
		t.Fatal("rate limit/referências")
	}
	now = now.Add(time.Millisecond)
	p.FeedCaptured(delta)
	if len(requests) != 2 || requests[1].Sub(requests[0]) != time.Second {
		t.Fatal(requests)
	}
	p.FeedCaptured(idr)
	p.FeedCaptured(delta)
	if r.broadcasts != 4 {
		t.Fatalf("não recuperou: %d", r.broadcasts)
	}
	// Novo drop dentro da mesma janela não pode pedir KF extra.
	r.setBuffered(congestionDropBytes + 1)
	p.FeedCaptured(delta)
	if len(requests) != 2 {
		t.Fatal(requests)
	}
}

func TestCapturedTimestampsFollowElapsedTimeIncludingDrops(t *testing.T) {
	p, r, peer := testPipeline(t)
	p.EnableEnvelope()
	start := time.Unix(100, 0)
	now := start
	manualVideoClock(p, &now)
	idr, delta := []byte{0, 0, 0, 1, 0x65, 1}, []byte{0, 0, 0, 1, 0x41, 2}
	// A espera do IDR antes da primeira AU não inicia o relógio.
	p.FeedCaptured(delta)
	now = now.Add(time.Second)
	p.FeedCaptured(idr)
	base := now
	for _, dt := range []time.Duration{33333 * time.Microsecond, 100 * time.Millisecond, 317 * time.Millisecond} {
		now = base.Add(dt)
		p.FeedCaptured(delta)
	}
	now = base.Add(700 * time.Millisecond)
	r.setBuffered(congestionDropBytes + 1)
	p.FeedCaptured(delta)
	r.setBuffered(0)
	now = base.Add(2 * time.Second)
	p.FeedCaptured(delta)
	p.FeedCaptured(idr)
	var timestamps []uint32
	for _, data := range r.packets {
		pkt, err := peer.Unprotect(data)
		if err != nil {
			t.Fatal(err)
		}
		timestamps = append(timestamps, pkt.Header.Timestamp)
	}
	if len(timestamps) != 5 {
		t.Fatal(timestamps)
	}
	for i, want := range []uint32{0, 2999, 9000, 28530, 180000} {
		if got := timestamps[i] - timestamps[0]; got != want {
			t.Fatalf("timestamp[%d]=%d quero=%d", i, got, want)
		}
	}
}

func TestLargeCapturedIDRFragmentationRoundtrip(t *testing.T) {
	p, r, peer := testPipeline(t)
	r.growBuffer = true // excede o orçamento DURANTE o IDR; não corta a AU
	au := append([]byte{0, 0, 0, 1, 0x67, 1, 0, 0, 0, 1, 0x68, 2, 0, 0, 0, 1, 0x65}, bytes.Repeat([]byte{7}, 120*1024)...)
	p.FeedCaptured(au)
	if len(r.packets) < 150 {
		t.Fatalf("IDR não fragmentado: %d", len(r.packets))
	}
	var depack transport.H264Depacketizer
	var rebuilt [][]byte
	var ts uint32
	var seq uint16
	for i, data := range r.packets {
		pkt, err := peer.Unprotect(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(pkt.Payload) > 800 || pkt.Header.Marker != (i == len(r.packets)-1) {
			t.Fatalf("fragmento=%d len=%d marker=%v", i, len(pkt.Payload), pkt.Header.Marker)
		}
		if i == 0 {
			ts, seq = pkt.Header.Timestamp, pkt.Header.SequenceNumber
		}
		if pkt.Header.Timestamp != ts || pkt.Header.SequenceNumber != seq+uint16(i) {
			t.Fatal("timestamp/seq inconsistentes")
		}
		if o, ok := parseVideoOrientation(pkt.Header); !ok || o != 0 || pkt.Header.ExtensionData[1]&videoMediaFrameIDR == 0 {
			t.Fatal("extensão IDR inválida")
		}
		wantHeader := byte(0x30)
		if i == 0 {
			wantHeader = 0x32
		}
		if pkt.Header.ExtensionData[0] != wantHeader {
			t.Fatal("FrameNumber fora do primeiro pacote")
		}
		rebuilt = append(rebuilt, depack.Depacketize(pkt.Payload)...)
	}
	if len(rebuilt) != 1 || !bytes.Equal(append([]byte{0, 0, 0, 1}, rebuilt[0]...), au) {
		t.Fatal("IDR grande não remontado integralmente")
	}
	before := r.broadcasts
	p.EnableEnvelope()
	r.setBuffered(congestionDropBytes + 1)
	p.FeedCaptured([]byte{0, 0, 0, 1, 0x41, 1})
	if r.broadcasts != before || !p.keyframeRequired {
		t.Fatal("congestão depois do IDR ignorada")
	}
}

func TestVideoConcurrentSendReceiveAndReset(t *testing.T) {
	p, _, peer := testPipeline(t)
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			p.FeedCaptured([]byte{0, 0, 0, 1, 0x65, 1})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			receivePacket(t, p, peer, uint16(i), uint32(i), true, []byte{0x65, 1}, nil)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			p.EnableEnvelope()
			p.SetOrientationFallback(i % 4)
			p.Reset()
			if err := p.Setup("CID", "999:1@lid", "111:1@lid", km, km); err != nil {
				t.Error(err)
			}
		}
	}()
	wg.Wait()
}

func TestReceiveIDRWaitCeilingAndSequenceRestart(t *testing.T) {
	p, _, peer := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var frames [][]byte
	p.OnFrame = func(au []byte) { frames = append(frames, au) }
	receivePacket(t, p, peer, 100, 1, true, []byte{0x65, 1}, nil)
	receivePacket(t, p, peer, 102, 2, true, []byte{0x41, 1}, nil)
	p.mu.Lock()
	p.drainReceiveLocked(now.Add(videoReorderWait))
	p.mu.Unlock()
	now = now.Add(2 * time.Second)
	receivePacket(t, p, peer, 103, 3, true, []byte{0x41, 2}, nil)
	if len(frames) != 2 || !p.receive.needIDR || !p.receive.idrWaitExpired {
		t.Fatal("IDR ceiling must forward deltas and keep requesting recovery")
	}
	for seq := uint16(1); seq <= 8; seq++ {
		receivePacket(t, p, peer, seq, uint32(seq+10), true, []byte{0x65, 3}, nil)
	}
	if p.receive.lastSeq != 8 || len(frames) != 3 {
		t.Fatal("same SSRC sequence restart did not recover")
	}
}

func TestCapturedLegacyAndQ1WaitForIDRAfterDrop(t *testing.T) {
	for _, q1 := range []bool{false, true} {
		p, r, _ := testPipeline(t)
		if q1 {
			p.EnableEnvelope()
		}
		now := time.Unix(100, 0)
		manualVideoClock(p, &now)
		requests := 0
		p.OnKeyframeRequired = func() { requests++ }
		p.FeedCaptured([]byte{0, 0, 0, 1, 0x65, 1})
		r.setBuffered(congestionDropBytes + 1)
		p.FeedCaptured([]byte{0, 0, 0, 1, 0x41, 1})
		r.setBuffered(0)
		p.FeedCaptured([]byte{0, 0, 0, 1, 0x41, 1})
		if q1 && r.broadcasts != 1 {
			t.Fatal("Q1 did not wait")
		}
		if !q1 && (r.broadcasts != 1 || requests != 1) {
			t.Fatal("legacy precisa KF e IDR após drop")
		}
		now = now.Add(2 * time.Second)
		p.FeedCaptured([]byte{0, 0, 0, 1, 0x41, 1})
		if r.broadcasts != 1 || !p.keyframeRequired {
			t.Fatal("delta sem referência após dois segundos")
		}
		p.FeedCaptured([]byte{0, 0, 0, 1, 0x65, 1})
		if r.broadcasts != 2 || p.keyframeRequired {
			t.Fatal("IDR não recuperou")
		}
	}
}
