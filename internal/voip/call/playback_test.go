package call

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

type fakeMediaTicker struct {
	ch chan time.Time
}

func (f *fakeMediaTicker) Ticks() <-chan time.Time { return f.ch }
func (*fakeMediaTicker) Stop()                     {}

type fakeMediaTimer struct {
	clock *fakePlaybackClock
	at    time.Duration
	fn    func()
	stop  bool
}

func (t *fakeMediaTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := !t.stop
	t.stop = true
	return was
}

type fakePlaybackClock struct {
	mu       sync.Mutex
	now      time.Duration
	timers   []*fakeMediaTimer
	interval time.Duration
	ticker   *fakeMediaTicker
}

func (c *fakePlaybackClock) NewTicker(d time.Duration) mediaTicker {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.interval = d
	c.ticker = &fakeMediaTicker{ch: make(chan time.Time)}
	return c.ticker
}
func (c *fakePlaybackClock) AfterFunc(d time.Duration, fn func()) mediaTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeMediaTimer{clock: c, at: c.now + d, fn: fn}
	c.timers = append(c.timers, t)
	return t
}
func (c *fakePlaybackClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now += d
	var due []func()
	for _, timer := range c.timers {
		if !timer.stop && timer.at <= c.now {
			timer.stop = true
			due = append(due, timer.fn)
		}
	}
	c.mu.Unlock()
	for _, fn := range due {
		fn()
	}
}

type playbackTestCodec struct {
	guardedTestCodec
	encoded chan []float32
}

func (c *playbackTestCodec) Encode(pcm []float32) ([]byte, error) {
	c.encoded <- slices.Clone(pcm)
	return []byte{1}, nil
}

func playbackManager(t *testing.T) (*CallManager, *fakePlaybackClock, *playbackTestCodec) {
	t.Helper()
	codec := &playbackTestCodec{encoded: make(chan []float32, 20)}
	m, _ := audioTestManager(t, codec)
	m.rtpSession = media.NewWhatsAppOpusSession(10)
	m.relay = &statsTestRelay{}
	clock := &fakePlaybackClock{}
	m.clock = clock
	t.Cleanup(func() { m.EndCall(context.Background(), core.EndCallReasonUserEnded) })
	return m, clock, codec
}

func playbackTick(t *testing.T, m *CallManager, clock *fakePlaybackClock, codec *playbackTestCodec) []float32 {
	t.Helper()
	clock.advance(60 * time.Millisecond)
	clock.mu.Lock()
	ticker, now := clock.ticker, clock.now
	clock.mu.Unlock()
	select {
	case ticker.ch <- time.Unix(0, int64(now)):
	case <-time.After(2 * time.Second):
		t.Fatal("send loop stuck")
	}
	var pcm []float32
	select {
	case pcm = <-codec.encoded:
	case <-time.After(2 * time.Second):
		t.Fatal("no encoded frame")
	}
	// Encode notification occurs under mu; this barrier waits for the relay send.
	m.mu.Lock()
	m.mu.Unlock()
	return pcm
}

func TestAnnounceAcceptsWithoutBrowserCadenceAndHangup(t *testing.T) {
	m, clock, codec := playbackManager(t)
	pcm := make([]float32, frame16+17)
	for i := range pcm {
		pcm[i] = float32(i+1) / 2000
	}
	if err := m.Announce(context.Background(), testIncomingCallID, media.WAVPCM{Samples: pcm, Rate: 16000}, true, time.Second); err != nil {
		t.Fatal(err)
	}
	if sd := m.VideoState(); sd.State != core.CallStateConnecting || m.browserAttached || clock.interval != 60*time.Millisecond {
		t.Fatalf("state=%+v interval=%s", sd, clock.interval)
	}
	// Relay and keys independently gate consumption, including before acceptance is active.
	m.mu.Lock()
	srtp := m.srtpSession
	m.srtpSession = nil
	m.currentCall.StateData.State = core.CallStateActive
	m.sendMediaFrameLocked(make([]float32, frame16))
	withoutKeys := m.playback.pos == 0 && len(codec.encoded) == 0
	m.srtpSession = srtp
	m.relay = &fakeRelay{}
	m.sendMediaFrameLocked(make([]float32, frame16))
	withoutRelay := m.playback.pos == 0 && len(codec.encoded) == 0
	m.relay = &statsTestRelay{}
	m.mu.Unlock()
	if !withoutKeys || !withoutRelay {
		t.Fatal("consumed audio without relay/SRTP")
	}
	first := playbackTick(t, m, clock, codec)
	if !slices.Equal(first, pcm[:frame16]) || m.VideoState().State == core.CallStateEnded {
		t.Fatal("first frame or premature hangup")
	}
	second := playbackTick(t, m, clock, codec)
	if !slices.Equal(second[:17], pcm[frame16:]) || second[17] != 0 || clock.now != 120*time.Millisecond {
		t.Fatal("last frame must be zero padded at 120ms")
	}
	clock.advance(299 * time.Millisecond)
	if m.VideoState().State == core.CallStateEnded {
		t.Fatal("hangup before 300ms tail")
	}
	clock.advance(time.Millisecond)
	if m.VideoState().State != core.CallStateEnded || m.DrainSendStats().SentFrames != 2 {
		t.Fatal("did not hang up after the last transmitted frame")
	}
}

func TestAnnounceSafetyTimeoutAndStaleTimer(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(map[bool]string{false: "waiting-media", true: "relay-lost"}[ready], func(t *testing.T) {
			m, clock, codec := playbackManager(t)
			wav := media.WAVPCM{Samples: make([]float32, 10*frame16), Rate: 16000}
			if err := m.Announce(context.Background(), testIncomingCallID, wav, true, time.Second); err != nil {
				t.Fatal(err)
			}
			if ready {
				m.mu.Lock()
				m.currentCall.StateData.State = core.CallStateActive
				m.mu.Unlock()
				playbackTick(t, m, clock, codec)
				clock.advance(2300 * time.Millisecond)
			} else {
				clock.advance(PlaybackReadyTimeout)
			}
			if m.VideoState().State != core.CallStateEnded || m.VideoState().EndReason != core.EndCallReasonTimeout {
				t.Fatal("safety timeout did not end call")
			}
		})
	}
	m, clock, _ := playbackManager(t)
	wav := media.WAVPCM{Samples: make([]float32, frame16), Rate: 16000}
	if err := m.Announce(context.Background(), testIncomingCallID, wav, true, time.Second); err != nil {
		t.Fatal(err)
	}
	oldTimer := clock.timers[0].fn
	m.ResetBrowserMedia(true)
	oldTimer() // callback already in flight when Stop returned
	if m.VideoState().State == core.CallStateEnded {
		t.Fatal("stale playback timer ended attached browser call")
	}
}

func TestHoldMusicLoopsAndStopsOnAttachOrResume(t *testing.T) {
	for _, mode := range []string{"attach", "resume"} {
		t.Run(mode, func(t *testing.T) {
			m, clock, codec := playbackManager(t)
			m.currentCall.StateData.State = core.CallStateActive
			wav := media.WAVPCM{Samples: []float32{.1, .2, .3}, Rate: 48000}
			if err := m.HoldMusic(testIncomingCallID, wav); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				frame := playbackTick(t, m, clock, codec)
				if len(frame) != frame16 || frame[0] != .2 || frame[frame16-1] != .2 {
					t.Fatal("hold WAV did not loop at codec rate")
				}
			}
			m.ResetBrowserMedia(false) // transfer detaches A but keeps the customized loop
			if frame := playbackTick(t, m, clock, codec); frame[0] != .2 {
				t.Fatal("transfer replaced customized hold music")
			}
			if mode == "attach" {
				m.ResetBrowserMedia(true)
			} else if err := m.Resume(); err != nil {
				t.Fatal(err)
			}
			m.mu.Lock()
			stopped := m.playback == nil && !m.held && m.currentCall.StateData.State == core.CallStateActive
			m.mu.Unlock()
			if !stopped {
				t.Fatal("hold music survived resume/attach")
			}
			if frame := playbackTick(t, m, clock, codec); frame[0] != 0 {
				t.Fatal("music still audible after stopping")
			}
		})
	}
}

func TestHoldMusicPreservesAnnounceHangup(t *testing.T) {
	m, clock, _ := playbackManager(t)
	wav := media.WAVPCM{Samples: make([]float32, frame16), Rate: 16000}
	if err := m.Announce(context.Background(), testIncomingCallID, wav, true, time.Second); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	original := m.playback
	m.currentCall.StateData.State = core.CallStateActive
	m.mu.Unlock()
	if err := m.HoldMusic(testIncomingCallID, wav); err == nil {
		t.Fatal("replaced announce")
	}
	m.mu.Lock()
	preserved := m.playback == original && m.playback.hangupAfter && !m.held
	m.mu.Unlock()
	if !preserved {
		t.Fatal("announce changed")
	}
	clock.advance(PlaybackReadyTimeout)
	if m.VideoState().State != core.CallStateEnded {
		t.Fatal("announce hangup lost")
	}
}
