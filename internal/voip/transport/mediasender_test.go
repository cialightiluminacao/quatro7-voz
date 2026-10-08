package transport

import (
	"bytes"
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
