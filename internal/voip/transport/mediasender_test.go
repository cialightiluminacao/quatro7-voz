package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"
)

type senderTestChannel struct {
	mu       sync.Mutex
	packets  [][]byte
	buffered uint64
	fail     bool
}

func (c *senderTestChannel) Send(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		c.fail = false
		return errors.New("send failed")
	}
	c.packets = append(c.packets, bytes.Clone(b))
	c.buffered += uint64(len(b))
	return nil
}

func TestVideoSendFailureSkipsDeltasQueuedBeforeRecoveryIDR(t *testing.T) {
	s, c := testMediaSender(t)
	now := time.Now()
	drops, sendErrors := 0, 0
	s.onError = func() { sendErrors++ }
	drop := func() { drops++ }
	s.enqueue(senderAU(now, true, drop, []byte{1}))
	s.enqueue(senderAU(now, false, drop, []byte{2}))
	c.fail = true
	s.step(now)
	// Um novo IDR chega antes do próximo tick. Não pode liberar a delta 2.
	s.enqueue(senderAU(now, true, drop, []byte{3}))
	s.enqueue(senderAU(now, false, drop, []byte{4}))
	s.step(now.Add(videoSendInterval))
	s.step(now.Add(2 * videoSendInterval))
	if drops != 2 || sendErrors != 1 || !bytes.Equal(bytes.Join(c.packets, nil), []byte{3, 4}) {
		t.Fatalf("drops=%d errors=%d packets=%v", drops, sendErrors, c.packets)
	}
}
func (c *senderTestChannel) BufferedAmount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buffered
}
func (c *senderTestChannel) drain() { c.mu.Lock(); c.buffered = 0; c.mu.Unlock() }

func TestRelayVideoReceiveDiagnosticsSeparateSourcesAndSilence(t *testing.T) {
	s1, _ := testMediaSender(t)
	s2, _ := testMediaSender(t)
	now := time.Unix(100, 0)
	video := make([]byte, 12)
	video[0], video[1] = 0x80, 97
	rtcp := []byte{0x9f, 206, 0, 1, 0, 0, 0, 42}
	s1.observeReceive(video, now)
	s2.observeReceive(rtcp, now.Add(time.Second))
	for _, invalid := range [][]byte{video[:8], {0, 97, 0, 0, 0, 0, 0, 0}, {0x80, 120, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}} {
		s1.observeReceive(invalid, now.Add(2*time.Second))
	}
	if s1.rxVideo.Load() != 1 || s1.rxRTCP.Load() != 0 || s2.rxVideo.Load() != 0 || s2.rxRTCP.Load() != 1 {
		t.Fatal("origens/categorias misturadas")
	}
	now = now.Add(5 * time.Second)
	if relayReceiveAge(now, s1.lastRXVideo.Load()) != 5000 || relayReceiveAge(now, s2.lastRXRTCP.Load()) != 4000 || relayReceiveAge(now, s2.lastRXVideo.Load()) != -1 {
		t.Fatal("silêncio não aparece por relay")
	}
}

func testMediaSender(t *testing.T) (*relayMediaSender, *senderTestChannel) {
	t.Helper()
	c := &senderTestChannel{}
	return newRelayMediaSender(c, slog.New(slog.NewTextHandler(io.Discard, nil)), "test", nil, nil), c
}
func senderAU(at time.Time, idr bool, drop func(), packets ...[]byte) *videoAccessUnit {
	au := &videoAccessUnit{packets: packets, idr: idr, at: at, onDrop: drop}
	for _, p := range packets {
		au.bytes += len(p)
	}
	return au
}

func TestVideoStaleDropSkipsDependentDeltasAndRecoversAtIDR(t *testing.T) {
	s, c := testMediaSender(t)
	now := time.Unix(100, 0)
	drops := 0
	drop := func() { drops++ }
	s.enqueue(senderAU(now, true, drop, []byte{1}, []byte{2}))
	s.enqueue(senderAU(now.Add(66*time.Millisecond), false, drop, []byte{3}))
	s.step(now) // começa IDR; rede para antes do último fragmento
	c.buffered = videoSCTPBytes
	s.step(now.Add(VideoMaxAge - time.Nanosecond))
	if drops != 0 {
		t.Fatal("descartou antes do limite")
	}
	s.enqueue(senderAU(now.Add(399*time.Millisecond), true, drop, []byte{4}))
	s.step(now.Add(VideoMaxAge))
	if drops != 2 || len(s.queue) != 1 || len(c.packets) != 1 {
		t.Fatalf("drops=%d queue=%d packets=%v", drops, len(s.queue), c.packets)
	}
	c.drain()
	s.step(now.Add(VideoMaxAge + time.Millisecond))
	s.enqueue(senderAU(now.Add(VideoMaxAge+2*time.Millisecond), false, drop, []byte{5}))
	c.drain()
	s.step(now.Add(VideoMaxAge + 2*time.Millisecond))
	if !bytes.Equal(bytes.Join(c.packets, nil), []byte{1, 4, 5}) || s.needIDR || s.bytes != 0 {
		t.Fatalf("recuperação=%v needIDR=%v bytes=%d", c.packets, s.needIDR, s.bytes)
	}
}

func TestVideoQueueBoundsAndSCTPAdmission(t *testing.T) {
	s, c := testMediaSender(t)
	now := time.Now()
	drops := 0
	for i := 0; i < 100; i++ {
		s.enqueue(senderAU(now, i%10 == 0, func() { drops++ }, make([]byte, 32<<10)))
		if s.bytes > videoQueueBytes || len(s.queue) > videoQueueFrames {
			t.Fatal("fila ilimitada")
		}
	}
	if drops == 0 {
		t.Fatal("overflow não descartou GOP")
	}
	s, c = testMediaSender(t)
	var packets [][]byte
	for i := 0; i < 11; i++ {
		packets = append(packets, make([]byte, 800))
	}
	s.enqueue(senderAU(now, true, nil, packets...))
	for i := 0; i < 20; i++ {
		s.step(now.Add(time.Duration(i) * videoSendInterval))
	}
	if len(c.packets) != 10 || c.BufferedAmount() > videoSCTPBytes {
		t.Fatal("vídeo inundou SCTP")
	}
	c.drain()
	s.step(now.Add(50 * time.Millisecond))
	if len(c.packets) != 11 {
		t.Fatal("não continuou após ACK")
	}
}

func TestVideoIDRCanCompleteWithinBudgetWith100msACKs(t *testing.T) {
	s, c := testMediaSender(t)
	now := time.Now()
	var packets [][]byte
	for i := 0; i < 30; i++ {
		packets = append(packets, make([]byte, 800))
	}
	drops := 0
	s.enqueue(senderAU(now, true, func() { drops++ }, packets...))
	for dt := time.Duration(0); dt < VideoMaxAge; dt += videoSendInterval {
		if dt%(100*time.Millisecond) == 0 {
			c.drain()
		}
		s.step(now.Add(dt))
	}
	if drops != 0 || len(c.packets) != len(packets) {
		t.Fatalf("IDR incompleta: packets=%d drops=%d", len(c.packets), drops)
	}
}

func TestAudioPriorityBeforeVideoAndThroughCongestion(t *testing.T) {
	s, c := testMediaSender(t)
	now := time.Now()
	s.enqueue(senderAU(now, true, nil, []byte("video1"), []byte("video2")))
	// Reserva o Send corrente, coloca áudio esperando, e tenta liberar vídeo.
	s.sendMu.Lock()
	done := make(chan error, 1)
	go func() { done <- s.sendPriority([]byte("audio")) }()
	deadline := time.Now().Add(time.Second)
	for s.priorityWaiting.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if s.priorityWaiting.Load() == 0 {
		s.sendMu.Unlock()
		t.Fatal("áudio não aguardou")
	}
	s.step(now)
	s.sendMu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.step(now.Add(videoSendInterval))
	if string(c.packets[0]) != "audio" || string(c.packets[1]) != "video1" {
		t.Fatal(c.packets)
	}
	// Um SCTP cheio suspende somente vídeo. Áudio continua sem esperar a fila AU.
	c.buffered = videoSCTPBytes
	s.step(now.Add(2 * videoSendInterval))
	if err := s.sendPriority([]byte("audio2")); err != nil {
		t.Fatal(err)
	}
	if len(c.packets) != 3 || string(c.packets[2]) != "audio2" {
		t.Fatal(c.packets)
	}
}

func TestVideoCancellationAndSenderShutdown(t *testing.T) {
	s, c := testMediaSender(t)
	now := time.Now()
	drops := 0
	au := senderAU(now, true, func() { drops++ }, []byte{1})
	au.valid = func() bool { return false }
	s.enqueue(au)
	s.step(now)
	if len(c.packets) != 0 || drops != 0 || s.bytes != 0 {
		t.Fatal("epoch cancelado enviado ou pediu KF")
	}
	stop := make(chan struct{})
	s.stop = stop
	go s.run()
	s.enqueue(senderAU(now, true, nil, []byte{2}))
	close(stop)
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("sender não parou")
	}
	s.enqueue(senderAU(now, true, nil, []byte{3}))
	if len(s.queue) != 0 || s.bytes != 0 {
		t.Fatal("fila sobreviveu cleanup")
	}
}

func TestVideoConcurrentProducersAudioAndDrops(t *testing.T) {
	s, c := testMediaSender(t)
	stop := make(chan struct{})
	s.stop = stop
	go s.run()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.enqueue(senderAU(time.Now(), j%15 == 0, nil, make([]byte, 800)))
				if err := s.sendPriority([]byte{1}); err != nil {
					t.Error(err)
				}
				c.drain()
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-s.done
}

func TestVideoDiagnosticsExposeAgePartialIDRAndQueueDrops(t *testing.T) {
	s, c := testMediaSender(t)
	now := time.Unix(100, 0)
	var packets [][]byte
	for i := 0; i < 50; i++ {
		packets = append(packets, make([]byte, 800))
	}
	s.enqueue(senderAU(now, true, nil, packets...)) // 40 KiB, ACK a cada 100 ms
	for dt := time.Duration(0); dt < VideoMaxAge; dt += videoSendInterval {
		if dt%(100*time.Millisecond) == 0 {
			c.drain()
		}
		s.step(now.Add(dt))
	}
	s.step(now.Add(VideoMaxAge))
	// Reproduz o limite, sem afirmar que o tamanho/RTT ocorreu em produção.
	if s.ausIn != 1 || s.ausSent != 0 || s.packetsSent != 40 || s.dropReasons["age"] != 1 || s.bufferWaits == 0 || !s.needIDR {
		t.Fatalf("in=%d sent=%d fragments=%d reasons=%v waits=%d", s.ausIn, s.ausSent, s.packetsSent, s.dropReasons, s.bufferWaits)
	}
	s.enqueue(senderAU(now.Add(VideoMaxAge), false, nil, []byte{1}))
	if s.dropReasons["waiting-idr"] != 1 {
		t.Fatal(s.dropReasons)
	}
	c.drain()
	s.enqueue(senderAU(now.Add(VideoMaxAge), true, nil, []byte{2}))
	s.step(now.Add(VideoMaxAge))
	if s.ausSent != 1 || s.needIDR {
		t.Fatal("IDR de recuperação não completou")
	}

	s, _ = testMediaSender(t)
	for i := 0; i < videoQueueFrames+1; i++ {
		s.enqueue(senderAU(now, true, nil, []byte{1}))
	}
	if s.dropReasons["queue-limit"] != videoQueueFrames || s.ausIn != videoQueueFrames+1 {
		t.Fatal(s.dropReasons)
	}
}

func TestBroadcastRTCPCountsSuccessfulRelaysAndErrors(t *testing.T) {
	s1, c1 := testMediaSender(t)
	s2, c2 := testMediaSender(t)
	c2.fail = true
	m := NewSctpRelayManager(s1.log)
	m.connections = map[string]*relayConnection{
		"one":    {state: relayStateOpen, sender: s1},
		"two":    {state: relayStateOpen, sender: s2},
		"closed": {state: relayStateClosed, sender: s1},
	}
	// Prefixo RR cifrado como o compound PLI: a proteção não altera PT/SSRC.
	packet := []byte{0x90, 201, 0, 1, 0, 0, 0, 42}
	if n := m.BroadcastRTCP(packet); n != 1 || len(c1.packets) != 1 || len(c2.packets) != 0 || m.DrainSendErrors() != 1 {
		t.Fatal("sucesso aparente em relay que falhou")
	}
	if s1.pliSent.Load() != 1 || s1.rtcpSent.Load() != 1 || s2.rtcpErrors.Load() != 1 {
		t.Fatal("contadores RTCP incorretos")
	}
	if n := m.BroadcastRTCP(packet); n != 2 || s2.pliSent.Load() != 1 {
		t.Fatal("relay recuperado não contado")
	}
}

func TestSlowVideoRelayDoesNotGateHealthyRelay(t *testing.T) {
	s1, c1 := testMediaSender(t)
	s2, c2 := testMediaSender(t)
	m := NewSctpRelayManager(s1.log)
	m.connections = map[string]*relayConnection{
		"healthy": {state: relayStateOpen, sender: s1},
		"slow":    {state: relayStateOpen, sender: s2},
	}
	now := time.Now()
	drops := 0
	onDrop := func() { drops++ }
	c2.buffered = videoSCTPBytes
	m.BroadcastVideoAU([][]byte{{1}, {2}}, true, now, nil, onDrop)
	s1.step(now)
	s1.step(now.Add(videoSendInterval)) // IDR inteira entregue pelo relay saudável
	s2.step(now.Add(VideoMaxAge))       // a cópia lenta expira
	if drops != 0 || s1.needIDR || !s2.needIDR || s1.ausSent != 1 || s2.dropReasons["age"] != 1 {
		t.Fatalf("drop isolado bloqueou pipeline: drops=%d healthy=%v slow=%v", drops, s1.needIDR, s2.needIDR)
	}
	m.BroadcastVideoAU([][]byte{{3}}, false, now.Add(VideoMaxAge), nil, onDrop)
	s1.step(now.Add(VideoMaxAge)) // delta continua pela cópia com referências válidas
	if drops != 0 || !bytes.Equal(bytes.Join(c1.packets, nil), []byte{1, 2, 3}) || s2.dropReasons["waiting-idr"] != 1 {
		t.Fatal("relay lento impediu continuação saudável")
	}
	// Se ambos descartam a mesma AU, pede exatamente uma recuperação global.
	c1.buffered = videoSCTPBytes
	m.BroadcastVideoAU([][]byte{{4}}, true, now.Add(VideoMaxAge), nil, onDrop)
	s1.step(now.Add(2 * VideoMaxAge))
	if drops != 0 {
		t.Fatal("recuperação antes de todos os destinos falharem")
	}
	s2.step(now.Add(2 * VideoMaxAge))
	if drops != 1 {
		t.Fatalf("recuperações globais=%d", drops)
	}
}

func TestVideoRelayGoneBetweenReadinessAndBroadcastRequestsRecovery(t *testing.T) {
	s, _ := testMediaSender(t)
	m := NewSctpRelayManager(s.log)
	drops := 0
	m.BroadcastVideoAU([][]byte{{1}}, true, time.Now(), nil, func() { drops++ })
	if drops != 1 {
		t.Fatal("AU perdida sem relay não pediu recuperação")
	}
}

func TestVideoAllRelaysDropConcurrentlyNotifiesOnce(t *testing.T) {
	s1, _ := testMediaSender(t)
	s2, _ := testMediaSender(t)
	m := NewSctpRelayManager(s1.log)
	m.connections = map[string]*relayConnection{
		"one": {state: relayStateOpen, sender: s1},
		"two": {state: relayStateOpen, sender: s2},
	}
	now := time.Now()
	dropped := make(chan struct{}, 2)
	m.BroadcastVideoAU([][]byte{{1}}, true, now, nil, func() {
		m.HasConnection() // callback fora dos locks dos destinos/manager
		dropped <- struct{}{}
	})
	var wg sync.WaitGroup
	for _, s := range []*relayMediaSender{s1, s2} {
		wg.Add(1)
		go func() { defer wg.Done(); s.step(now.Add(VideoMaxAge)) }()
	}
	wg.Wait()
	if len(dropped) != 1 {
		t.Fatalf("notificações concorrentes=%d", len(dropped))
	}
}

func TestVideoStoppedRelayReportsQueuedAndNewLoss(t *testing.T) {
	s, _ := testMediaSender(t)
	stop := make(chan struct{})
	s.stop = stop
	now := time.Now()
	drops := 0
	s.enqueue(senderAU(now, true, func() { drops++ }, []byte{1}))
	close(stop)
	go s.run()
	<-s.done
	if drops != 1 || s.dropReasons["relay-closed"] != 1 {
		t.Fatal("AU enfileirada perdida no fechamento sem recuperação")
	}
	// Snapshot pode conter um sender que fechou antes do enqueue.
	s.enqueue(senderAU(now, true, func() { drops++ }, []byte{2}))
	if drops != 2 || s.dropReasons["relay-closed"] != 2 || len(s.queue) != 0 {
		t.Fatal("admissão depois do fechamento perdeu AU sem recuperação")
	}
}

func TestVideoSenderReportStatsExcludeUnsentFragmentsAndRelayDuplicates(t *testing.T) {
	s1, _ := testMediaSender(t)
	s2, _ := testMediaSender(t)
	now := time.Now()
	packet := make([]byte, 12+10+4) // payload 10 bytes + tag SRTP 4
	packet[0], packet[1] = 0x80, 97
	binary.BigEndian.PutUint32(packet[8:12], 42)
	m := NewSctpRelayManager(s1.log)
	m.connections = map[string]*relayConnection{
		"one": {state: relayStateOpen, sender: s1},
		"two": {state: relayStateOpen, sender: s2},
	}
	for _, s := range []*relayMediaSender{s1, s2} {
		s.enqueue(senderAU(now, true, nil, packet, packet))
		s.step(now)
	}
	if packets, octets := m.VideoSenderStats(42); packets != 1 || octets != 10 {
		t.Fatalf("relays duplicados/fragmento pendente: %d/%d", packets, octets)
	}
	s1.step(now.Add(videoSendInterval))
	if packets, octets := m.VideoSenderStats(42); packets != 2 || octets != 20 {
		t.Fatalf("destino que avançou: %d/%d", packets, octets)
	}
	s2.step(now.Add(VideoMaxAge)) // segundo fragmento nunca saiu por este relay
	if packets, octets := m.VideoSenderStats(42); packets != 2 || octets != 20 {
		t.Fatalf("drop contado no SR: %d/%d", packets, octets)
	}
	if packets, _ := m.VideoSenderStats(43); packets != 0 {
		t.Fatal("outro SSRC contado")
	}
}
