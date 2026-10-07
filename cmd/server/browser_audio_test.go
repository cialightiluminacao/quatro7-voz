package main

import (
	"sync/atomic"
	"testing"
	"time"

	"wacalls/internal/voip/media"
)

type browserTestCodec struct {
	entered    chan struct{}
	release    chan struct{}
	closed     atomic.Bool
	active     atomic.Int64
	decodes    atomic.Int64
	violations atomic.Int64
}

func (*browserTestCodec) Encode([]float32) ([]byte, error) { return []byte{1}, nil }
func (*browserTestCodec) FrameSize() int                   { return 960 }
func (*browserTestCodec) SampleRate() int                  { return 48000 }
func (c *browserTestCodec) Decode([]byte) ([]float32, error) {
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
	return make([]float32, 960), nil
}

func (c *browserTestCodec) Close() {
	if c.active.Load() != 0 || c.closed.Swap(true) {
		c.violations.Add(1)
	}
}

func waitBrowserTest(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("mídia do navegador travou")
	}
}

func TestBrowserDecodeSerializedWithSwapAndTeardown(t *testing.T) {
	for _, mode := range []string{"webrtc", "ws", "remove", "drain"} {
		t.Run(mode, func(t *testing.T) {
			codec := &browserTestCodec{entered: make(chan struct{}), release: make(chan struct{})}
			replacement := &browserTestCodec{}
			ac := &activeCall{browserOpus: codec}
			s := &Session{reg: newCallRegistry()}
			s.reg.add("c1", ac)
			done := make(chan struct{})
			go func() { s.decodeBrowserAudio("c1", ac, codec, []byte{1}); close(done) }()
			waitBrowserTest(t, codec.entered)
			changed := make(chan struct{})
			go func() {
				switch mode {
				case "webrtc":
					s.setBridge("c1", nil, replacement)
				case "ws":
					s.setWSBridge("c1", &wsBridge{closed: true, cancel: func() {}}, replacement)
				case "remove":
					s.removeCall("c1")
				case "drain":
					for _, drained := range s.reg.drain() {
						drained.closeMedia()
					}
				}
				close(changed)
			}()
			select {
			case <-changed:
				t.Error("Close/swap terminou com Decode em voo")
			case <-time.After(20 * time.Millisecond):
			}
			close(codec.release)
			waitBrowserTest(t, done)
			waitBrowserTest(t, changed)
			if pcm := s.decodeBrowserAudio("c1", ac, codec, []byte{1}); pcm != nil {
				t.Fatal("callback antigo usou codec após Close")
			}
			if !codec.closed.Load() || codec.decodes.Load() != 1 || codec.violations.Load() != 0 {
				t.Fatalf("closed=%v decodes=%d violations=%d", codec.closed.Load(), codec.decodes.Load(), codec.violations.Load())
			}
			if mode == "webrtc" || mode == "ws" {
				if len(s.decodeBrowserAudio("c1", ac, replacement, []byte{1})) != 960 {
					t.Fatal("codec novo não recebeu áudio")
				}
				s.removeCall("c1")
			}
		})
	}
}

func TestBrowserConcurrentCallbacksAreSerialized(t *testing.T) {
	c := &browserTestCodec{entered: make(chan struct{}), release: make(chan struct{})}
	ac := &activeCall{browserOpus: c}
	s := &Session{reg: newCallRegistry()}
	s.reg.add("c1", ac)
	first, second := make(chan struct{}), make(chan struct{})
	go func() { s.decodeBrowserAudio("c1", ac, c, []byte{1}); close(first) }()
	waitBrowserTest(t, c.entered)
	go func() { s.decodeBrowserAudio("c1", ac, c, []byte{2}); close(second) }()
	close(c.release)
	waitBrowserTest(t, first)
	waitBrowserTest(t, second)
	s.removeCall("c1")
	if c.decodes.Load() != 2 || c.violations.Load() != 0 {
		t.Fatalf("decodes=%d violations=%d", c.decodes.Load(), c.violations.Load())
	}
}

func TestWSBridgeCloseDoesNotHoldMediaMu(t *testing.T) {
	for _, mode := range []string{"swap", "remove", "drain"} {
		t.Run(mode, func(t *testing.T) {
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			codec, replacement := &browserTestCodec{}, &browserTestCodec{}
			// Bloqueia dentro de Close sem depender do timeout de uma conexão real.
			bridge := &wsBridge{closed: true, cancel: func() {
				close(entered)
				<-release
			}}
			ac := &activeCall{wsBridge: bridge, browserOpus: codec}
			s := &Session{reg: newCallRegistry()}
			s.reg.add("c1", ac)
			defer func() {
				close(release)
				waitBrowserTest(t, done)
				s.removeCall("c1")
			}()
			go func() {
				switch mode {
				case "swap":
					s.setWSBridge("c1", &wsBridge{closed: true, cancel: func() {}}, replacement)
				case "remove":
					s.removeCall("c1")
				case "drain":
					for _, drained := range s.reg.drain() {
						drained.closeMedia()
					}
				}
				close(done)
			}()
			waitBrowserTest(t, entered)
			if !ac.mediaMu.TryLock() {
				t.Fatal("Close do WS bloqueado segurou mediaMu")
			}
			ac.mediaMu.Unlock()
			if !codec.closed.Load() || codec.violations.Load() != 0 {
				t.Fatal("codec antigo não foi fechado antes de Close do WS")
			}
			if pcm := s.decodeBrowserAudio("c1", ac, codec, []byte{1}); pcm != nil {
				t.Fatal("callback antigo usou codec durante Close do WS")
			}
			if mode == "swap" && len(s.decodeBrowserAudio("c1", ac, replacement, []byte{1})) != 960 {
				t.Fatal("Close do WS impediu áudio no codec novo")
			}
		})
	}
}

var _ media.Codec = (*browserTestCodec)(nil)
