package call

import (
	"time"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

func (m *CallManager) initCodec() {
	if m.codec != nil {
		return
	}
	codec, err := media.NewMLowCodec(media.DefaultCodecOptions)
	if err != nil {
		m.log.Warn("MLow codec unavailable — call will run signaling-only (no audio)", "err", err)
		return
	}
	m.codec = codec
}

func (m *CallManager) FeedCapturedPCM(data []float32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.playback != nil {
		return
	}
	m.feedCapturedPCMLocked(data)
}

func (m *CallManager) feedCapturedPCMLocked(data []float32) {
	if m.codec == nil || len(data) == 0 {
		return
	}
	m.sendStatsMu.Lock()
	defer m.sendStatsMu.Unlock()
	m.sendStats.CaptureFrames++
	m.captureBuf = append(m.captureBuf, data...)
	if maxBuffered := m.codec.FrameSize() * 4; len(m.captureBuf) > maxBuffered {
		m.sendStats.CaptureDroppedSamples += int64(len(m.captureBuf) - maxBuffered)
		m.captureBuf = m.captureBuf[len(m.captureBuf)-maxBuffered:]
	}
}

func (m *CallManager) sendOpusFrameLocked(opus []byte) bool {
	if m.rtpSession == nil || m.srtpSession == nil {
		return false
	}
	marker := !m.firstPacketSent
	pkt := m.rtpSession.CreatePacketWithDuration(opus, m.codec.FrameSize(), marker)
	if m.debeEnabled {
		pkt.Header.Extension = true
		pkt.Header.ExtensionProfile = 0xbede
		pkt.Header.ExtensionData = nil
	}
	m.firstPacketSent = true

	srtp, err := m.srtpSession.Protect(pkt)
	if err != nil {
		m.noteSendError()
		m.log.Debug("srtp protect error", "err", err)
		return false
	}
	m.noteSendLocked(time.Now())
	m.relay.Broadcast(srtp)
	return true
}

func (m *CallManager) startMediaSendLoopLocked() {
	if m.sendLoopStop != nil || m.codec == nil {
		return
	}
	stop := make(chan struct{})
	m.sendLoopStop = stop
	frameSize := m.codec.FrameSize()
	ticker := m.mediaClock().NewTicker(mediaFrameDuration)
	go func() {
		defer ticker.Stop()
		silence := make([]float32, frameSize)
		for {
			select {
			case <-stop:
				return
			case <-ticker.Ticks():
			}
			m.mu.Lock()
			if m.sendLoopStop != stop {
				m.mu.Unlock()
				return
			}
			m.sendMediaFrameLocked(silence)
			m.mu.Unlock()
		}
	}()
}

// sendMediaFrameLocked mantém a seleção/agregação histórica (mu travado).
func (m *CallManager) sendMediaFrameLocked(silence []float32) {
	frameSize := len(silence)
	if m.codec == nil || m.rtpSession == nil || m.srtpSession == nil || !m.relay.HasConnection() {
		return
	}
	frame := silence
	switch {
	case m.playback != nil:
		if m.currentCall == nil || (m.currentCall.StateData.State != core.CallStateActive && m.currentCall.StateData.State != core.CallStateOnHold) {
			return
		}
		frame = m.playbackFrameLocked(frameSize)
	case m.held:
		// Em espera: não envia o mic do atendente. Toca música de espera
		// (ou silêncio) para o interlocutor manter o leg vivo.
		if m.mohEnabled {
			frame = m.nextMohFrameLocked(frameSize)
		}
	case len(m.captureBuf) >= frameSize:
		frame = make([]float32, frameSize)
		copy(frame, m.captureBuf[:frameSize])
		m.captureBuf = m.captureBuf[frameSize:]
	default:
		m.sendStatsMu.Lock()
		m.sendStats.CaptureUnderruns++
		m.sendStatsMu.Unlock()
	}
	started := time.Now()
	opus, err := m.codec.Encode(frame)
	encodeDuration := time.Since(started)
	m.sendStatsMu.Lock()
	m.sendStats.EncodeMax = max(m.sendStats.EncodeMax, encodeDuration)
	m.sendStatsMu.Unlock()
	if err == nil {
		if m.sendOpusFrameLocked(opus) && m.playback != nil {
			m.sendStatsMu.Lock()
			m.sendStats.CaptureFrames++
			m.sendStatsMu.Unlock()
			m.playbackSentLocked(frameSize)
		}
	} else {
		m.noteSendError()
	}
}

// nextMohFrameLocked devolve o próximo quadro (frameSize amostras) do loop de música
// de espera, avançando a posição. Deve ser chamado com m.mu travado.
func (m *CallManager) nextMohFrameLocked(frameSize int) []float32 {
	frame := make([]float32, frameSize)
	if len(mohLoop) == 0 {
		return frame
	}
	for i := 0; i < frameSize; i++ {
		frame[i] = mohLoop[m.mohPos]
		m.mohPos++
		if m.mohPos >= len(mohLoop) {
			m.mohPos = 0
		}
	}
	return frame
}

func (m *CallManager) onRelayData(data []byte) {
	if transport.IsStunPacket(data) {
		return
	}
	if media.IsRTCPPacket(data) {
		m.video.HandleRTCP(data)
		return
	}
	if !transport.IsRtpPacket(data) {
		return
	}
	if len(data) < 12 {
		return
	}
	switch data[1] & 0x7f {
	case core.PayloadTypeWhatsAppOpus:
		m.handleAudioRelayData(data)
	case core.PayloadTypeWhatsAppH264:
		m.video.HandleRelayData(data)
	}
}

func (m *CallManager) handleAudioRelayData(data []byte) {
	m.audioReceiveMu.Lock()
	defer m.audioReceiveMu.Unlock()
	m.mu.Lock()
	if m.srtpSession == nil || m.codec == nil {
		m.mu.Unlock()
		return
	}
	header, err := media.DecodeRtpHeader(data)
	if err != nil {
		m.mu.Unlock()
		m.log.Debug("rtp header error", "err", err)
		return
	}
	ssrc := header.Ssrc
	if ssrc == m.selfSsrc {
		m.mu.Unlock()
		return
	}
	// Nenhum pacote sem autenticação pode alterar a fila, SSRC ou timeline.
	pkt, err := m.srtpSession.Unprotect(data)
	if err != nil {
		m.mu.Unlock()
		m.log.Debug("srtp unprotect error", "err", err)
		return
	}
	if len(pkt.Payload) == 0 {
		m.mu.Unlock()
		return
	}
	if !m.actualPeerSet {
		m.actualPeerSet = true
		if !containsSsrc(m.peerSsrcs, ssrc) {
			m.peerSsrcs = []uint32{ssrc}
			m.relay.SetSubscriptionSsrc(ssrc)
			go m.relay.ResendSubscriptions()
		}
	}
	now := time.Now()
	duplicate, late, resynced := m.audioReorder.push(pkt.Header, pkt.Payload, now)
	if resynced {
		m.audioTimelineSet = false
		m.audioPlayedSamples = 0
	}
	if duplicate || late {
		m.peerLateDrops.Add(1)
		if duplicate {
			m.peerDuplicates.Add(1)
		}
		m.mu.Unlock()
		return
	}
	pcm := m.drainPeerAudioLocked(now)
	m.schedulePeerAudioLocked()
	m.mu.Unlock()
	m.deliverPeerAudio(pcm)
}

func (m *CallManager) deliverPeerAudio(pcm []float32) {
	if m.OnPeerAudio != nil && len(pcm) > 0 {
		m.OnPeerAudio(pcm)
	}
}

// Tanto o relay quanto o timer usam audioReceiveMu -> mu, inclusive até o callback.
func (m *CallManager) schedulePeerAudioLocked() {
	if m.audioReorderTimer != nil {
		m.audioReorderTimer.Stop()
		m.audioReorderTimer = nil
	}
	m.audioReceiveEpoch++
	if len(m.audioReorder.pending) == 0 {
		return
	}
	epoch := m.audioReceiveEpoch
	m.audioReorderTimer = time.AfterFunc(time.Until(m.audioReorder.deadline()), func() {
		m.audioReceiveMu.Lock()
		defer m.audioReceiveMu.Unlock()
		m.mu.Lock()
		if epoch != m.audioReceiveEpoch || m.codec == nil || m.srtpSession == nil {
			m.mu.Unlock()
			return
		}
		pcm := m.drainPeerAudioLocked(time.Now())
		m.schedulePeerAudioLocked()
		m.mu.Unlock()
		m.deliverPeerAudio(pcm)
	})
}

func (m *CallManager) drainPeerAudioLocked(now time.Time) []float32 {
	var out []float32
	for {
		p, ok := m.audioReorder.pop(now)
		if !ok {
			break
		}
		if p.reordered {
			m.peerReordered.Add(1)
		}
		if len(p.payload) == 0 {
			continue
		}
		ts := p.header.Timestamp
		if m.peerAudioLateLocked(ts) {
			continue
		}
		// PLC ANTES do frame seguinte: no máximo 3 frames, nunca zeros de padding.
		var plc []float32
		gap, frameSize := m.peerAudioGapLocked(ts), m.codec.FrameSize()
		if p.missing > 0 && frameSize > 0 && gap > 0 && gap <= int64(maxPeerPLCFrames*frameSize) && gap%int64(frameSize) == 0 {
			for n := int64(0); n < gap/int64(frameSize); n++ {
				pcm, err := m.codec.Decode(nil)
				if err != nil {
					break
				}
				plc = append(plc, pcm...)
			}
		}
		pcm, err := m.codec.Decode(p.payload)
		if err != nil || len(pcm) == 0 {
			m.audioTimelineSet = false
			continue
		}
		pcm = m.alignPeerAudioLocked(ts, pcm)
		// Em hold o decoder/timeline avançam, sem entregar áudio ao navegador.
		if !m.held {
			out = append(out, plc...)
			out = append(out, pcm...)
		}
	}
	return out
}

func (m *CallManager) resetPeerAudioLocked(cleanup bool) {
	m.audioReceiveEpoch++
	if m.audioReorderTimer != nil {
		m.audioReorderTimer.Stop()
		m.audioReorderTimer = nil
	}
	if cleanup {
		m.audioReorder = peerAudioReorder{}
	} else {
		m.audioReorder.discardPending()
	}
	m.audioTimelineSet = false
	m.audioPlayedSamples = 0
}

// Contadores cumulativos lidos sem mu pelo log de playout.
func (m *CallManager) PeerLateDrops() int64  { return m.peerLateDrops.Load() }
func (m *CallManager) PeerReordered() int64  { return m.peerReordered.Load() }
func (m *CallManager) PeerDuplicates() int64 { return m.peerDuplicates.Load() }

func (m *CallManager) alignPeerAudio(ts uint32, pcm []float32) []float32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alignPeerAudioLocked(ts, pcm)
}

const (
	maxPeerGapSamples = 8000
	maxPeerPLCFrames  = 3
)

func (m *CallManager) peerAudioGapLocked(ts uint32) int64 {
	if !m.audioTimelineSet {
		return 0
	}
	return int64(int32(ts - (m.audioBaseTs + uint32(m.audioPlayedSamples))))
}

// Segunda barreira antes de Decode para timestamps atrasados, mesmo com seq nova.
func (m *CallManager) peerAudioLateLocked(ts uint32) bool {
	gap := m.peerAudioGapLocked(ts)
	if gap < 0 && -gap <= maxPeerGapSamples {
		m.peerLateDrops.Add(1)
		return true
	}
	return false
}

// Alinha apenas a referência temporal. Lacunas nunca acrescentam silêncio ao PCM.
func (m *CallManager) alignPeerAudioLocked(ts uint32, pcm []float32) []float32 {
	if m.peerAudioLateLocked(ts) {
		return nil
	}
	gap := m.peerAudioGapLocked(ts)
	if !m.audioTimelineSet || gap < 0 || gap > maxPeerGapSamples {
		m.audioTimelineSet = true
		m.audioBaseTs = ts
		m.audioPlayedSamples = uint64(len(pcm))
	} else {
		m.audioPlayedSamples = uint64(ts-m.audioBaseTs) + uint64(len(pcm))
	}
	return pcm
}
