package call

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

// Detecta Decode concorrente e qualquer uso durante/depois de Close sem depender
// de um segfault nativo. O gate mantém um decode em voo durante o teardown.
type guardedTestCodec struct {
	decodes    atomic.Int64
	active     atomic.Int64
	closed     atomic.Bool
	violations atomic.Int64
	entered    chan struct{}
	release    chan struct{}
	encodeErr  error
	encodedPCM [][]float32
}

func (c *guardedTestCodec) Encode(pcm []float32) ([]byte, error) {
	c.encodedPCM = append(c.encodedPCM, slices.Clone(pcm))
	return []byte{1}, c.encodeErr
}

func (c *guardedTestCodec) Decode([]byte) ([]float32, error) {
	if c.active.Add(1) != 1 || c.closed.Load() {
		c.violations.Add(1)
	}
	defer c.active.Add(-1)
	if c.decodes.Add(1) == 1 && c.entered != nil {
		close(c.entered)
		<-c.release
	}
	if c.closed.Load() {
		c.violations.Add(1)
	}
	return pcmFrame(), nil
}

func (*guardedTestCodec) FrameSize() int  { return frame16 }
func (*guardedTestCodec) SampleRate() int { return 16000 }
func (c *guardedTestCodec) Close() {
	if c.active.Load() != 0 || c.closed.Swap(true) {
		c.violations.Add(1)
	}
}

func audioTestManager(t *testing.T, codec media.Codec) (*CallManager, *media.SrtpSession) {
	t.Helper()
	m := incomingRingingManager()
	m.codec = codec
	m.selfSsrc, m.actualPeerSet = 10, true
	key := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	var err error
	m.srtpSession, err = media.NewSrtpSession(key, key, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := media.NewSrtpSession(key, key, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	return m, tx
}

func audioTestPacket(t *testing.T, tx *media.SrtpSession, seq uint16, ts uint32) []byte {
	t.Helper()
	pkt, err := tx.Protect(&media.RtpPacket{
		Header:  media.NewRtpHeader(core.PayloadTypeWhatsAppOpus, seq, ts, 20),
		Payload: []byte{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return pkt
}

func waitMediaTest(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("operação de mídia travou (ordem de locks)")
	}
}

func TestAudioDecodeWaitsBeforeClose(t *testing.T) {
	c := &guardedTestCodec{entered: make(chan struct{}), release: make(chan struct{})}
	m, tx := audioTestManager(t, c)
	// Callback pode adquirir mu: handleAudioRelayData deve liberá-lo antes.
	m.OnPeerAudio = func([]float32) { m.FeedCapturedPCM(nil) }
	packet := audioTestPacket(t, tx, 0, 0)
	received := make(chan struct{})
	go func() { m.handleAudioRelayData(packet); close(received) }()
	waitMediaTest(t, c.entered)
	cleaned := make(chan struct{})
	go func() { m.cleanupMedia(); close(cleaned) }()
	select {
	case <-cleaned:
		t.Error("cleanup terminou com Decode ainda em voo")
	case <-time.After(20 * time.Millisecond):
	}
	close(c.release)
	waitMediaTest(t, received)
	waitMediaTest(t, cleaned)
	m.handleAudioRelayData(packet) // callback tardio após Close deve ser inofensivo
	if !c.closed.Load() || c.decodes.Load() != 1 || c.violations.Load() != 0 {
		t.Fatalf("closed=%v decodes=%d violations=%d", c.closed.Load(), c.decodes.Load(), c.violations.Load())
	}
}

func TestAudioConcurrentRelaysDecodeDuplicateOnce(t *testing.T) {
	c := &guardedTestCodec{entered: make(chan struct{}), release: make(chan struct{})}
	m, tx := audioTestManager(t, c)
	packet := audioTestPacket(t, tx, 0, 0)
	var delivered atomic.Int64
	m.OnPeerAudio = func([]float32) { delivered.Add(1) }
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { m.handleAudioRelayData(packet) })
	}
	waitMediaTest(t, c.entered)
	close(c.release)
	wg.Wait()
	m.cleanupMedia()
	if c.decodes.Load() != 1 || delivered.Load() != 1 || m.PeerLateDrops() != 7 || c.violations.Load() != 0 {
		t.Fatalf("decodes=%d delivered=%d late=%d violations=%d", c.decodes.Load(), delivered.Load(), m.PeerLateDrops(), c.violations.Load())
	}
}

func TestAudioDropsLateBeforeDecoderAndPreservesPadding(t *testing.T) {
	c := &guardedTestCodec{}
	m, tx := audioTestManager(t, c)
	defer m.cleanupMedia()
	var lengths []int
	m.OnPeerAudio = func(pcm []float32) { lengths = append(lengths, len(pcm)) }
	packets := make([][]byte, 5)
	for i := range packets {
		packets[i] = audioTestPacket(t, tx, uint16(i), uint32(i*frame16))
	}
	for _, i := range []int{0, 1, 3, 2, 4, 4} {
		m.handleAudioRelayData(packets[i])
	}
	if !slices.Equal(lengths, []int{frame16, frame16, 2 * frame16, frame16}) || c.decodes.Load() != 4 || m.PeerLateDrops() != 2 {
		t.Fatalf("PCM lengths=%v decodes=%d late=%d", lengths, c.decodes.Load(), m.PeerLateDrops())
	}
}

func TestAudioHeldDuplicateDoesNotReachDecoder(t *testing.T) {
	c := &guardedTestCodec{}
	m, tx := audioTestManager(t, c)
	defer m.cleanupMedia()
	m.held = true
	first := audioTestPacket(t, tx, 0, 0)
	m.handleAudioRelayData(first)
	m.handleAudioRelayData(first)
	m.handleAudioRelayData(audioTestPacket(t, tx, 1, frame16))
	m.held = false
	var delivered int
	m.OnPeerAudio = func(pcm []float32) { delivered = len(pcm) }
	m.handleAudioRelayData(audioTestPacket(t, tx, 2, 2*frame16))
	if c.decodes.Load() != 3 || m.PeerLateDrops() != 1 || delivered != frame16 {
		t.Fatalf("decodes=%d late=%d resumed=%d", c.decodes.Load(), m.PeerLateDrops(), delivered)
	}
}

type statsTestRelay struct {
	fakeRelay
	errors atomic.Int64
	sent   int
}

func (*statsTestRelay) HasConnection() bool      { return true }
func (r *statsTestRelay) Broadcast([]byte)       { r.sent++ }
func (r *statsTestRelay) DrainSendErrors() int64 { return r.errors.Swap(0) }

func TestSendDiagnosticsPreserveCaptureAggregation(t *testing.T) {
	c := &guardedTestCodec{}
	m, _ := audioTestManager(t, c)
	defer m.cleanupMedia()
	m.rtpSession = media.NewWhatsAppOpusSession(10)
	r := &statsTestRelay{}
	m.relay = r
	// 5 quadros: o teto histórico mantém os 4 mais recentes, sem alterar pacing.
	pcm := make([]float32, 5*frame16)
	for i := range pcm {
		pcm[i] = float32(i / frame16)
	}
	m.FeedCapturedPCM(pcm[:320])
	m.FeedCapturedPCM(pcm[320:])
	m.mu.Lock()
	for range 5 { // 4 capturados + 1 silêncio de underrun
		m.sendMediaFrameLocked(make([]float32, frame16))
	}
	c.encodeErr = errors.New("encode de teste")
	m.sendMediaFrameLocked(make([]float32, frame16))
	m.mu.Unlock()
	r.errors.Add(2)
	stats := m.DrainSendStats()
	if stats.CaptureFrames != 2 || stats.CaptureDroppedSamples != frame16 || stats.CaptureUnderruns != 2 || stats.SentFrames != 5 || stats.Errors != 3 || stats.EncodeMax <= 0 || r.sent != 5 {
		t.Fatalf("stats=%+v relay.sent=%d", stats, r.sent)
	}
	for i := range 4 {
		if c.encodedPCM[i][0] != float32(i+1) {
			t.Fatalf("quadro %d mudou: %v", i, c.encodedPCM[i][0])
		}
	}
	if c.encodedPCM[4][0] != 0 || len(m.captureBuf) != 0 {
		t.Fatal("underrun/agregação mudaram")
	}
	if next := m.DrainSendStats(); next != (SendStats{}) {
		t.Fatalf("drenagem repetiu contadores: %+v", next)
	}
}

func TestSendJitterUsesActualIntervalsAcrossLogPeriods(t *testing.T) {
	m := &CallManager{}
	base := time.Unix(100, 0)
	m.noteSendLocked(base)
	m.noteSendLocked(base.Add(70 * time.Millisecond))
	m.noteSendLocked(base.Add(125 * time.Millisecond))
	stats := m.DrainSendStats()
	if stats.JitterIntervals != 2 || stats.JitterMax != 10*time.Millisecond || stats.JitterTotal != 15*time.Millisecond {
		t.Fatalf("stats=%+v", stats)
	}
	m.noteSendLocked(base.Add(190 * time.Millisecond))
	if next := m.DrainSendStats(); next.JitterIntervals != 1 || next.JitterTotal != 5*time.Millisecond {
		t.Fatalf("perdeu intervalo entre períodos: %+v", next)
	}
}

func TestSendStatsCanDrainWhileCallMutexIsHeld(t *testing.T) {
	m := &CallManager{}
	m.mu.Lock() // OnStateChange pode esperar o playout terminar com mu travado.
	defer m.mu.Unlock()
	done := make(chan struct{})
	go func() { m.DrainSendStats(); close(done) }()
	waitMediaTest(t, done)
}

func TestSendStatsConcurrentCaptureAndDrainPreservesCounters(t *testing.T) {
	m := &CallManager{codec: &guardedTestCodec{}}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 200 {
			m.FeedCapturedPCM(make([]float32, 320))
		}
	})
	var frames, dropped int64
	for range 100 {
		stats := m.DrainSendStats()
		frames += stats.CaptureFrames
		dropped += stats.CaptureDroppedSamples
	}
	wg.Wait()
	stats := m.DrainSendStats()
	frames += stats.CaptureFrames
	dropped += stats.CaptureDroppedSamples
	if frames != 200 || dropped != 200*320-4*frame16 {
		t.Fatalf("perdeu contadores na drenagem concorrente: frames=%d dropped=%d", frames, dropped)
	}
}
