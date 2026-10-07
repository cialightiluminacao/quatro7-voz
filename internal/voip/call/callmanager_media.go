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

func (m *CallManager) sendOpusFrameLocked(opus []byte) {
	if m.rtpSession == nil || m.srtpSession == nil {
		return
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
		return
	}
	m.noteSendLocked(time.Now())
	m.relay.Broadcast(srtp)
}

func (m *CallManager) startMediaSendLoopLocked() {
	if m.sendLoopStop != nil || m.codec == nil {
		return
	}
	stop := make(chan struct{})
	m.sendLoopStop = stop
	frameSize := m.codec.FrameSize()
	go func() {
		ticker := time.NewTicker(60 * time.Millisecond)
		defer ticker.Stop()
		silence := make([]float32, frameSize)
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
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
		m.sendOpusFrameLocked(opus)
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
	ssrc := media.RTPSsrc(data)
	if ssrc == m.selfSsrc {
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
	pkt, err := m.srtpSession.Unprotect(data)
	if err != nil {
		m.mu.Unlock()
		m.log.Debug("srtp unprotect error", "err", err)
		return
	}
	if len(pkt.Payload) == 0 || m.peerAudioLateLocked(pkt.Header.Timestamp) {
		m.mu.Unlock()
		return
	}
	pcm, err := m.codec.Decode(pkt.Payload)
	if err != nil || len(pcm) == 0 {
		m.mu.Unlock()
		return
	}
	// Mesmo em hold o decoder avança: registrar a timeline impede que o segundo
	// relay decodifique de novo o mesmo pacote e mantém a retomada na posição atual.
	pcm = m.alignPeerAudioLocked(pkt.Header.Timestamp, pcm)
	if m.held {
		m.mu.Unlock()
		// Em espera: não encaminha o áudio do peer ao navegador do atendente.
		return
	}
	m.mu.Unlock()
	if m.OnPeerAudio != nil && len(pcm) > 0 {
		m.OnPeerAudio(pcm)
	}
}

// PeerLateDrops conta os pacotes de áudio do peer descartados por atraso/duplicata.
func (m *CallManager) PeerLateDrops() int64 { return m.peerLateDrops.Load() }

func (m *CallManager) alignPeerAudio(ts uint32, pcm []float32) []float32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alignPeerAudioLocked(ts, pcm)
}

const maxPeerGapSamples = 8000

// peerAudioLateLocked roda ANTES de Decode: descartar depois já teria alterado
// o estado do decoder. Mantém a janela frota6c e permite reinícios grandes.
func (m *CallManager) peerAudioLateLocked(ts uint32) bool {
	if !m.audioTimelineSet {
		return false
	}
	gap := int64(uint64(ts-m.audioBaseTs)) - int64(m.audioPlayedSamples)
	if gap < 0 && -gap <= maxPeerGapSamples {
		m.peerLateDrops.Add(1)
		return true
	}
	return false
}

func (m *CallManager) alignPeerAudioLocked(ts uint32, pcm []float32) []float32 {
	if m.peerAudioLateLocked(ts) {
		return nil
	}
	origLen := uint64(len(pcm))
	if !m.audioTimelineSet {
		m.audioTimelineSet = true
		m.audioBaseTs = ts
		m.audioPlayedSamples = origLen
		return pcm
	}
	target := uint64(ts - m.audioBaseTs)
	gap := int64(target) - int64(m.audioPlayedSamples)
	if gap < 0 || gap > maxPeerGapSamples {
		m.audioBaseTs = ts
		m.audioPlayedSamples = origLen
		return pcm
	}
	if gap > 0 {
		padded := make([]float32, int(gap)+int(origLen))
		copy(padded[int(gap):], pcm)
		pcm = padded
	}
	m.audioPlayedSamples = target + origLen
	return pcm
}
