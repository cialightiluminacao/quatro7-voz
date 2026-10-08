package video

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"sync"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

func protectVideoPacket(t *testing.T, peer *media.SrtpSession, seq uint16, ts uint32, marker bool, payload []byte) []byte {
	t.Helper()
	h := media.NewRtpHeader(97, seq, ts, 42)
	h.Marker = marker
	b, err := peer.Protect(&media.RtpPacket{Header: h, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestEncryptedFUAReorderAndTwoRelayDuplicates(t *testing.T) {
	for _, base := range []uint16{100, 65533} {
		t.Run(strconv.Itoa(int(base)), func(t *testing.T) {
			p, r, peer := testPipeline(t)
			p.EnableEnvelope()
			var frames [][]byte
			p.OnFrame = func(b []byte) { p.EnableEnvelope(); frames = append(frames, b) }
			receivePacket(t, p, peer, base, 1, true, []byte{0x65, 1}, nil)
			nalu := append([]byte{0x41}, bytes.Repeat([]byte{7}, 2400)...)
			fragments := transport.PackageH264NALU(nalu)
			var packets [][]byte
			for i, f := range fragments {
				packets = append(packets, protectVideoPacket(t, peer, base+1+uint16(i), 2, i == len(fragments)-1, f))
			}
			// Um relay adianta marker; o outro entrega a lacuna e uma rajada velha.
			for _, i := range []int{0, 2, 3, 2, 1, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3} {
				p.HandleRelayData(packets[i])
			}
			if len(frames) != 2 || !bytes.Equal(frames[1][12:], append([]byte{0, 0, 0, 1}, nalu...)) || p.receive.needIDR || r.broadcasts != 0 {
				t.Fatalf("frames=%d needIDR=%v pedidos=%d", len(frames), p.receive.needIDR, r.broadcasts)
			}
			if p.reorder.duplicates < 8 || p.reorder.missing != 0 {
				t.Fatal("duplicatas/reordenação não contabilizadas")
			}
		})
	}
}

func TestPeerKeyframeRateLimitAndRecoveryWithoutRotation(t *testing.T) {
	p, r, peer := testPipeline(t)
	p.EnableEnvelope()
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var frames [][]byte
	p.OnFrame = func(b []byte) { frames = append(frames, b) }
	// SPS/PPS/IDR juntos como WhatsApp; nenhuma mudança de resolução/orientação.
	receivePacket(t, p, peer, 100, 1, true, []byte{0x67, 1, 0, 0, 0, 1, 0x68, 2, 0, 0, 0, 1, 0x65, 3}, nil)
	receivePacket(t, p, peer, 101, 2, false, []byte{0x7c, 0x81, 1}, nil)
	receivePacket(t, p, peer, 103, 2, true, []byte{0x7c, 0x41, 3}, nil)
	if r.broadcasts != 0 {
		t.Fatal("pediu IDR antes de aguardar reorder")
	}
	now = now.Add(videoReorderWait)
	p.flushReceive(p.receiveEpoch)
	if r.broadcasts != 1 || !p.receive.needIDR || len(frames) != 1 {
		t.Fatal("perda real não pediu IDR")
	}
	for i := 0; i < 20; i++ {
		p.RequestPeerKeyframe()
	}
	if r.broadcasts != 1 {
		t.Fatal("rajada KF excedeu 1/s")
	}
	now = now.Add(time.Second - time.Nanosecond)
	p.RequestPeerKeyframe()
	if r.broadcasts != 1 {
		t.Fatal("pedido antes de 1 segundo")
	}
	now = now.Add(time.Nanosecond)
	p.flushReceive(p.receiveEpoch)
	if r.broadcasts != 2 {
		t.Fatal("timer sem novo RTP não repetiu pedido")
	}
	now = now.Add(time.Second)
	receivePacket(t, p, peer, 104, 3, true, []byte{0x41, 4}, nil)
	if len(frames) != 2 || frames[1][3] != 0 || !p.receive.needIDR || !p.receive.idrWaitExpired || r.broadcasts != 3 {
		t.Fatal("timeout precisa liberar delta completa mantendo PLI")
	}
	// O PLI mira SSRC autenticado real (42), e não o SSRC calculado.
	k := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	rx, err := media.NewSrtcpSession(k, k)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range r.packets {
		plain, err := rx.Unprotect(b)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(plain, media.BuildVideoKeyframeRequest(p.selfSsrc, 42)) || binary.BigEndian.Uint32(b[len(b)-14:len(b)-10]) != 0x80000001+uint32(i) {
			t.Fatalf("pedido %d=%x", i, plain)
		}
	}
	receivePacket(t, p, peer, 105, 4, true, []byte{0x65, 5}, nil)
	want := []byte{0, 0, 0, 1, 0x67, 1, 0, 0, 0, 1, 0x68, 2, 0, 0, 0, 1, 0x65, 5}
	if len(frames) != 3 || frames[2][3] != 1 || !bytes.Equal(frames[2][12:], want) || p.receive.needIDR || p.receive.idrWaitExpired || p.receiveTimer != nil {
		t.Fatal("IDR solicitado não recuperou SPS/PPS e parou timer")
	}
	now = now.Add(time.Second)
	receivePacket(t, p, peer, 106, 5, true, []byte{0x41, 6}, nil)
	if len(frames) != 4 || r.broadcasts != 3 {
		t.Fatal("delta depois do IDR não retomou")
	}
}

func TestPendingKFUnauthenticatedPacketsAndResetTimers(t *testing.T) {
	p, r, peer := testPipeline(t)
	p.RequestPeerKeyframe()
	if r.broadcasts != 0 {
		t.Fatal("pedido sem SSRC autenticado")
	}
	b := protectVideoPacket(t, peer, 100, 1, true, []byte{0x41, 1})
	bad := bytes.Clone(b)
	bad[len(bad)-1] ^= 1
	p.HandleRelayData(bad)
	if p.reorder.set || p.receive.seqKnown || r.broadcasts != 0 {
		t.Fatal("pacote adulterado alterou estado")
	}
	p.HandleRelayData(b)
	if r.broadcasts != 1 {
		t.Fatal("KF pendente não foi enviado após autenticar")
	}
	oldEpoch := p.receiveEpoch
	p.Reset()
	p.flushReceive(oldEpoch)
	p.RequestPeerKeyframe()
	if r.broadcasts != 1 || p.receiveTimer != nil || len(p.reorder.pending) > 0 {
		t.Fatal("timer/pedido sobreviveu Reset")
	}
}

func TestReorderTimerFlushesWithoutNewPackets(t *testing.T) {
	p, _, peer := testPipeline(t)
	receivePacket(t, p, peer, 100, 1, true, []byte{0x65, 1}, nil)
	delivered := make(chan []byte, 1)
	p.OnFrame = func(b []byte) { delivered <- b }
	// IDR completa depois de um pacote perdido: timer deve liberar sem mais RTP.
	receivePacket(t, p, peer, 102, 2, true, []byte{0x65, 2}, nil)
	select {
	case b := <-delivered:
		if !bytes.Equal(b, []byte{0, 0, 0, 1, 0x65, 2}) {
			t.Fatal("AU errada")
		}
	case <-time.After(time.Second):
		t.Fatal("fila parada sem novo pacote")
	}
}

func TestReorderTimerSharesBridgeReceiveGuard(t *testing.T) {
	p, _, peer := testPipeline(t)
	var guard sync.Mutex
	p.ReceiveGuard = &guard
	receivePacket(t, p, peer, 100, 1, true, []byte{0x65, 1}, nil)
	delivered := make(chan struct{}, 1)
	p.OnFrame = func([]byte) { delivered <- struct{}{} }
	guard.Lock()
	// Arma a fila sob o guard para não depender de o scheduler levar menos
	// de 60 ms entre a chegada de um pacote e a aquisição do lock no teste.
	queued := reorderPacket(102, 2, 2)
	queued.Header.Marker = true
	queued.Payload = []byte{0x65, 2}
	p.mu.Lock()
	p.reorder.push(queued, p.now())
	p.scheduleReceiveLocked()
	p.mu.Unlock()
	select {
	case <-delivered:
		guard.Unlock()
		t.Fatal("timer ignorou troca de bridge")
	case <-time.After(2 * videoReorderWait):
	}
	guard.Unlock()
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("timer não retomou após guard")
	}
}

func TestTimeoutNeverForwardsDamagedAUOrFakeKey(t *testing.T) {
	p, _, peer := testPipeline(t)
	p.EnableEnvelope()
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	var frames [][]byte
	p.OnFrame = func(b []byte) { frames = append(frames, b) }
	receivePacket(t, p, peer, 1, 1, true, []byte{0x65, 1}, nil)
	p.RequestPeerKeyframe()
	now = now.Add(3 * time.Second)
	receivePacket(t, p, peer, 2, 2, true, []byte{0x7c, 0x41, 1}, nil) // FU órfão após expiração
	if len(frames) != 1 {
		t.Fatal("FU danificado entregue no timeout")
	}
	receivePacket(t, p, peer, 3, 3, true, []byte{0x41, 2}, nil)
	if len(frames) != 2 || frames[1][3] != 0 || !p.receive.needIDR {
		t.Fatal("delta completa marcada chave ou interrompeu recuperação")
	}
}

func TestAuthenticatedPeerRTCPFeedback(t *testing.T) {
	p, _, _ := testPipeline(t)
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	requests := 0
	p.OnKeyframeRequired = func() { requests++; p.EnableEnvelope() }
	k := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	peer, err := media.NewSrtcpSession(k, k)
	if err != nil {
		t.Fatal(err)
	}
	protect := func(plain []byte) []byte {
		t.Helper()
		b, err := peer.Protect(plain)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	b := protect(media.BuildVideoPLI(42, p.selfSsrc))
	bad := bytes.Clone(b)
	bad[len(bad)-1] ^= 1
	p.HandleRTCP(bad)
	p.HandleRTCP(protect(media.BuildVideoPLI(42, p.selfSsrc+1)))
	if requests != 0 {
		t.Fatal("feedback adulterado/outro alvo pediu encoder")
	}
	p.HandleRTCP(b)
	p.HandleRTCP(b)
	if requests != 1 {
		t.Fatal("feedback real/duplicado")
	}
	p.HandleRTCP(protect(media.BuildVideoPLI(42, p.selfSsrc)))
	if requests != 1 {
		t.Fatal("feedback não rate-limited")
	}
	now = now.Add(time.Second)
	fir := []byte{0x94, 206, 0, 4, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}
	binary.BigEndian.PutUint32(fir[12:16], p.selfSsrc)
	p.HandleRTCP(protect(fir))
	if requests != 2 || !p.keyframeRequired {
		t.Fatal("FIR não pediu nosso encoder")
	}
	now = now.Add(time.Second)
	p.HandleRTCP(protect(append(media.BuildVideoPLI(42, p.selfSsrc), 0x80, 206, 0, 3)))
	if requests != 2 {
		t.Fatal("compound truncado disparou callback")
	}
}

func TestRepeatedSetupPreservesSrtcpIndexAndReplay(t *testing.T) {
	p, r, peer := testPipeline(t)
	receivePacket(t, p, peer, 1, 1, true, []byte{0x65, 1}, nil)
	p.RequestPeerKeyframe()
	k := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	control, _ := media.NewSrtcpSession(k, k)
	b, _ := control.Protect(media.BuildVideoPLI(42, p.selfSsrc))
	requests := 0
	p.OnKeyframeRequired = func() { requests++ }
	p.HandleRTCP(b)
	if err := p.Setup("CID", "999:1@lid", "111:1@lid", k, k); err != nil {
		t.Fatal(err)
	}
	p.HandleRTCP(b)
	if requests != 1 {
		t.Fatal("Setup apagou proteção replay")
	}
	// Verifica índice do segundo pedido, sem esperar de verdade 1s.
	p.mu.Lock()
	p.lastPeerKFAt = time.Time{}
	p.mu.Unlock()
	receivePacket(t, p, peer, 2, 2, true, []byte{0x41, 1}, nil)
	if len(r.packets) != 2 || binary.BigEndian.Uint32(r.packets[1][len(r.packets[1])-14:len(r.packets[1])-10]) != 0x80000002 {
		t.Fatal("Setup reutilizou IV SRTCP")
	}
}
