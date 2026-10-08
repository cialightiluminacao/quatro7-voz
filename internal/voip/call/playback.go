package call

import (
	"context"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

const mediaFrameDuration = 60 * time.Millisecond
const PlaybackReadyTimeout = 15 * time.Second
const playbackTail = 300 * time.Millisecond

type mediaTicker interface {
	Ticks() <-chan time.Time
	Stop()
}
type mediaTimer interface{ Stop() bool }
type mediaClock interface {
	NewTicker(time.Duration) mediaTicker
	AfterFunc(time.Duration, func()) mediaTimer
}
type realMediaClock struct{}
type realMediaTicker struct{ *time.Ticker }

func (t realMediaTicker) Ticks() <-chan time.Time { return t.C }
func (realMediaClock) NewTicker(d time.Duration) mediaTicker {
	return realMediaTicker{time.NewTicker(d)}
}
func (realMediaClock) AfterFunc(d time.Duration, fn func()) mediaTimer {
	return time.AfterFunc(d, fn)
}
func (m *CallManager) mediaClock() mediaClock {
	if m.clock != nil {
		return m.clock
	}
	return realMediaClock{}
}

type serverPlayback struct {
	pcm         []float32
	pos         int
	loop        bool
	hangupAfter bool
	started     bool
	maxDuration time.Duration
	readyTimer  mediaTimer
	safetyTimer mediaTimer
	finishTimer mediaTimer
}

// Announce reuses AcceptCall for incoming calls and never waits for browser WebRTC.
// Only one source can own the capture path; the send loop clocks it at 60 ms.
func (m *CallManager) Announce(ctx context.Context, callID string, wav media.WAVPCM, hangupAfter bool, maxDuration time.Duration) error {
	m.mu.Lock()
	c := m.currentCall
	if c == nil || c.CallID != callID || c.IsEnded() || c.Direction != core.CallDirectionIncoming || m.browserAttached || m.playback != nil || m.held || (!c.CanAccept() && c.StateData.State != core.CallStateConnecting && !c.IsActive()) {
		m.mu.Unlock()
		return &CallError{"announce requires an incoming call without a browser or another playback"}
	}
	if maxDuration <= 0 || maxDuration > media.MaxWAVDuration || wav.Duration() > maxDuration {
		m.mu.Unlock()
		return &CallError{"audio duration exceeds maxSeconds"}
	}
	p, err := m.newPlaybackLocked(wav, false, hangupAfter, maxDuration)
	accept := c.CanAccept()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if accept {
		if err := m.AcceptCall(ctx, callID); err != nil {
			m.mu.Lock()
			if m.playback == p {
				m.stopPlaybackLocked()
			}
			m.mu.Unlock()
			return err
		}
	}
	m.mu.Lock()
	if m.playback == p {
		m.startMediaSendLoopLocked()
	}
	m.mu.Unlock()
	return nil
}

// HoldMusic requires the server to detach the browser first. It suppresses peer
// delivery and loops until Resume, a browser attachment, or termination.
func (m *CallManager) HoldMusic(callID string, wav media.WAVPCM) error {
	m.audioReceiveMu.Lock()
	defer m.audioReceiveMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.currentCall
	if c == nil || c.CallID != callID || c.IsEnded() || m.browserAttached || (!c.IsActive() && c.StateData.State != core.CallStateOnHold) {
		return &CallError{"hold-music requires an active call without a browser"}
	}
	if m.playback != nil && !m.playback.loop {
		return &CallError{"hold-music cannot replace an active announce"}
	}
	if _, err := m.newPlaybackLocked(wav, true, false, 0); err != nil {
		return err
	}
	if c.IsActive() {
		_ = c.ApplyTransition(Transition{Type: TransitionHold})
	}
	m.held, m.mohEnabled = true, false
	m.resetPeerAudioLocked(false)
	if m.OnPeerAudioReset != nil {
		m.OnPeerAudioReset()
	}
	m.emitState()
	m.startMediaSendLoopLocked()
	return nil
}

func (m *CallManager) newPlaybackLocked(wav media.WAVPCM, loop, hangup bool, maxDuration time.Duration) (*serverPlayback, error) {
	if m.codec == nil || len(wav.Samples) == 0 {
		return nil, &CallError{"audio codec or PCM unavailable"}
	}
	pcm, err := wav.AtRate(m.codec.SampleRate())
	if err != nil {
		return nil, err
	}
	m.stopPlaybackLocked()
	m.captureBuf = nil
	p := &serverPlayback{pcm: pcm, loop: loop, hangupAfter: hangup, maxDuration: maxDuration}
	m.playback = p
	p.readyTimer = m.mediaClock().AfterFunc(PlaybackReadyTimeout, func() { m.expirePlayback(p) })
	return p, nil
}

func (m *CallManager) expirePlayback(p *serverPlayback) {
	if p.hangupAfter {
		m.endCall(context.Background(), core.EndCallReasonTimeout, p)
		return
	}
	m.mu.Lock()
	if m.playback == p {
		if p.loop && !p.started {
			m.log.Warn("hold-music expired without media")
		}
		m.stopPlaybackLocked()
	}
	m.mu.Unlock()
}

func (m *CallManager) stopPlaybackLocked() {
	if p := m.playback; p != nil {
		for _, timer := range []mediaTimer{p.readyTimer, p.safetyTimer, p.finishTimer} {
			if timer != nil {
				timer.Stop()
			}
		}
	}
	m.playback = nil
}

func (m *CallManager) playbackFrameLocked(n int) []float32 {
	p := m.playback
	if !p.started {
		p.started = true
		p.readyTimer.Stop()
		if !p.loop {
			p.safetyTimer = m.mediaClock().AfterFunc(p.maxDuration+playbackTail+time.Second, func() { m.expirePlayback(p) })
		}
	}
	frame := make([]float32, n)
	for i := range frame {
		pos := p.pos + i
		if p.loop {
			pos %= len(p.pcm)
		}
		if pos < len(p.pcm) {
			frame[i] = p.pcm[pos]
		}
	}
	return frame
}

func (m *CallManager) playbackSentLocked(n int) {
	p := m.playback
	p.pos += n
	if p.loop {
		p.pos %= len(p.pcm)
		return
	}
	if p.pos >= len(p.pcm) && p.finishTimer == nil {
		if p.hangupAfter {
			p.finishTimer = m.mediaClock().AfterFunc(playbackTail, func() {
				m.endCall(context.Background(), core.EndCallReasonUserEnded, p)
			})
		} else {
			m.stopPlaybackLocked()
		}
	}
}

// ResetBrowserMedia runs before the registry swap, outside mediaMu. The epoch
// fences Q1 and captured audio that were already in flight from an old bridge.
func (m *CallManager) ResetBrowserMedia(attached bool) uint64 {
	m.browserVideoMu.Lock()
	defer m.browserVideoMu.Unlock()
	m.audioReceiveMu.Lock()
	defer m.audioReceiveMu.Unlock()
	m.peerVideoReceiveMu.Lock()
	defer m.peerVideoReceiveMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.browserEpoch++
	m.browserAttached = attached
	m.video.DisableEnvelope()
	m.captureBuf = nil
	m.resetPeerAudioLocked(false)
	if m.OnPeerAudioReset != nil {
		m.OnPeerAudioReset()
	}
	if attached {
		music := m.playback != nil && m.playback.loop
		m.stopPlaybackLocked()
		if music && m.currentCall != nil && m.currentCall.StateData.State == core.CallStateOnHold {
			_ = m.currentCall.ApplyTransition(Transition{Type: TransitionResume})
			m.held, m.mohEnabled = false, false
			m.emitState()
		}
	}
	return m.browserEpoch
}

func (m *CallManager) EnableVideoEnvelopeForBridge(callID string, epoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if epoch != 0 && epoch == m.browserEpoch && m.browserAttached && m.currentCall != nil && m.currentCall.CallID == callID && !m.currentCall.IsEnded() {
		m.video.EnableEnvelope()
	}
}

func (m *CallManager) FeedBrowserPCM(epoch uint64, pcm []float32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if epoch != 0 && epoch == m.browserEpoch && m.browserAttached {
		m.feedCapturedPCMLocked(pcm)
	}
}

func (m *CallManager) FeedBrowserVideo(epoch uint64, au []byte) {
	m.browserVideoMu.Lock()
	defer m.browserVideoMu.Unlock()
	m.mu.Lock()
	valid := epoch != 0 && epoch == m.browserEpoch && m.browserAttached && m.currentCall != nil && !m.currentCall.IsEnded()
	m.mu.Unlock()
	if valid {
		m.video.FeedCaptured(au)
	}
}
