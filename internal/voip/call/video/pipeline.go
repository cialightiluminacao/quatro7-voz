package video

import (
	"encoding/binary"
	"log/slog"
	"sync"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

const (
	// Duas IDRs de até 128 KiB. Verificado antes da AU; nunca corta seus fragmentos.
	congestionDropBytes = 256 << 10
	slotWord            = 2
)

var (
	callSlots       = []uint32{0, 1, 4, 2, 3, 5}
	annexBStartCode = []byte{0, 0, 0, 1}
)

type Relay interface {
	Broadcast(data []byte)
	BufferedAmount() uint64
	HasConnection() bool
	SetStreamSsrcs(selfSsrcs, peerSsrcs []uint32)
}

type Pipeline struct {
	log   *slog.Logger
	relay Relay

	sendMu     sync.Mutex // serializa packetização e SRTP de envio fora de mu
	mu         sync.Mutex
	receiveMu  sync.Mutex // serializa recepção e callbacks; nunca adquirido sob mu
	rtp        *media.RtpSession
	srtp       *media.SrtpSession
	selfSsrc   uint32
	depack     *transport.H264Depacketizer
	frameBuf   []byte
	firstAUAt  time.Time
	clockTicks uint32
	now        func() time.Time
	lastKFAt   time.Time
	idrWaitAt  time.Time

	callID                 string
	envelopeCapable        bool
	peerOrientation        int
	rtpOrientationKnown    bool
	receive                receiveState
	receiveSPS, receivePPS []byte

	// Estado da extensão de vídeo do WhatsApp (perfil 0xDEBE) no envio.
	frameNumber      uint16 // nº do frame (por access unit), começa em 1
	transportSeq     uint16 // sequência transport (por pacote)
	keyframeRequired bool   // o primeiro frame enviado precisa ser IDR

	OnFrame func(au []byte)
	// Callbacks rodam sem mu; Setup/Reset podem ser chamados com o lock da call.
	OnOrientation      func(callID string, orientation int)
	OnKeyframeRequired func()
}

func New(log *slog.Logger, relay Relay) *Pipeline {
	if log == nil {
		log = slog.Default()
	}
	return &Pipeline{log: log, relay: relay, now: time.Now}
}

func (p *Pipeline) Setup(callID, ourDeviceJid, peerDeviceJid string, sendKM, recvKM core.SrtpKeyingMaterial) error {
	srtp, err := media.NewSrtpSession(sendKM, recvKM, core.SRTPSendAuthTagLen, core.SRTPRecvAuthTagLen)
	if err != nil {
		return err
	}
	selfSsrc := media.GenerateSecureSsrc(callID, ourDeviceJid, slotWord)

	selfSsrcs := make([]uint32, len(callSlots))
	peerSsrcs := make([]uint32, len(callSlots))
	for i, slot := range callSlots {
		selfSsrcs[i] = media.GenerateSecureSsrc(callID, ourDeviceJid, slot)
		peerSsrcs[i] = media.GenerateSecureSsrc(callID, peerDeviceJid, slot)
	}

	p.mu.Lock()
	if p.callID != callID {
		p.resetLocked()
		p.callID = callID
	}
	p.srtp = srtp
	p.selfSsrc = selfSsrc
	p.rtp = media.NewH264Session(selfSsrc)
	p.depack = &transport.H264Depacketizer{}
	p.receive = receiveState{auSequence: p.receive.auSequence, needIDR: p.receive.seqKnown || p.receive.needIDR, idrWaitAt: p.receive.idrWaitAt}
	p.frameBuf = nil
	p.frameNumber = 1
	p.transportSeq = 0
	p.keyframeRequired = true
	p.firstAUAt = time.Time{}
	p.clockTicks = 0
	p.mu.Unlock()

	p.relay.SetStreamSsrcs(selfSsrcs, peerSsrcs)
	p.log.Debug("video media set up", "self_video_ssrc", selfSsrc,
		"stream_ssrcs", len(selfSsrcs)+len(peerSsrcs))
	return nil
}

// videoExtProfile é o perfil da RTP header extension do vídeo do WhatsApp (0xDEBE,
// NÃO 0xBEDE). O conteúdo (one-byte headers) traz MediaFrameInfo, InitialBandwidth,
// ShortOffset e TransportSequence — não abs-send-time. Formato confirmado contra o
// vídeo real do WhatsApp e o meowcaller/whatsapp-rust.
const videoExtProfile = 0xDEBE

const (
	videoMediaFrameIDR   = 0x08 // frame é IDR/keyframe
	videoMediaFrameDelta = 0x20 // frame delta
)

// buildWhatsappVideoExt monta o bloco da extensão 0xDEBE (padded a múltiplo de 4).
// frameNumber só entra no PRIMEIRO pacote de cada access unit (ptr não-nil).
func buildWhatsappVideoExt(mediaFrameInfo uint8, frameNumber *uint16, transportSeq uint16) []byte {
	ext := make([]byte, 0, 16)
	if frameNumber != nil {
		ext = append(ext, 0x32, mediaFrameInfo, byte(*frameNumber>>8), byte(*frameNumber)) // id3, len3
	} else {
		ext = append(ext, 0x30, mediaFrameInfo) // id3, len1
	}
	ext = append(ext, 0x51, 0x00, 0x00)                                // id5 InitialBandwidth=0
	ext = append(ext, 0x61, 0x00, 0x00)                                // id6 ShortOffset=0
	ext = append(ext, 0x91, byte(transportSeq>>8), byte(transportSeq)) // id9 TransportSequence
	for len(ext)%4 != 0 {
		ext = append(ext, 0x00) // padding até fronteira de 4 bytes
	}
	return ext
}

func (p *Pipeline) FeedCaptured(au []byte) {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	p.mu.Lock()
	locked := true
	var requestKeyframe func()
	defer func() {
		if locked {
			p.mu.Unlock()
		}
		if requestKeyframe != nil {
			requestKeyframe()
		}
	}()
	rtp, srtp := p.rtp, p.srtp
	frameNum := p.frameNumber
	tseq := p.transportSeq
	if rtp == nil || srtp == nil || !p.relay.HasConnection() || len(au) == 0 {
		return
	}
	nalus := transport.SplitAnnexB(au)
	if len(nalus) == 0 {
		return
	}
	idr := hasIDR(nalus)
	now := p.now()
	// Após um drop, repete KF enquanto espera IDR, no máximo uma vez por segundo.
	if p.relay.BufferedAmount() > congestionDropBytes {
		if p.envelopeCapable {
			if !p.keyframeRequired || p.idrWaitAt.IsZero() {
				p.idrWaitAt = now
			}
			p.keyframeRequired = true
			requestKeyframe = p.requestKeyframeLocked(now)
		}
		return
	}
	// Aguarda o IDR inicial ou a recuperação Q1, com teto de dois segundos.
	if p.keyframeRequired && !idr {
		if p.idrWaitAt.IsZero() {
			p.idrWaitAt = now
		}
		if now.Sub(p.idrWaitAt) < 2*time.Second {
			if p.envelopeCapable {
				requestKeyframe = p.requestKeyframeLocked(now)
			}
			return
		}
		p.log.Info("video outbound IDR wait expired", "idr_wait_ms", now.Sub(p.idrWaitAt).Milliseconds())
		p.keyframeRequired = false
		p.idrWaitAt = time.Time{}
	}
	// Junta TODOS os NALUs (menos AUD tipo 9) num único access unit Annex-B e fragmenta
	// como UM NAL — é assim que o WhatsApp manda o vídeo (não NALU a NALU).
	var packed []byte
	for _, n := range nalus {
		if len(n) == 0 || n[0]&0x1f == 9 {
			continue
		}
		if len(packed) > 0 {
			packed = append(packed, 0, 0, 0, 1)
		}
		packed = append(packed, n...)
	}
	if len(packed) == 0 {
		return
	}
	if p.firstAUAt.IsZero() {
		p.firstAUAt = now
	} else {
		// Âncora na primeira AU enviada: inclui pausas/drops e não acumula erros
		// de arredondamento. time.Now.Sub usa o relógio monotônico quando disponível.
		elapsed := now.Sub(p.firstAUAt)
		if elapsed > 0 {
			ticks := uint32(elapsed/time.Second)*90000 + uint32(elapsed%time.Second*90000/time.Second)
			rtp.AdvanceTimestamp(ticks - p.clockTicks)
			p.clockTicks = ticks
		}
	}
	mediaFrameInfo := uint8(videoMediaFrameDelta)
	if idr {
		mediaFrameInfo = videoMediaFrameIDR
	}
	payloads := transport.PackageH264NALU(packed)
	packets := make([]*media.RtpPacket, 0, len(payloads))
	for i, payload := range payloads {
		last := i == len(payloads)-1
		var fn *uint16
		if i == 0 {
			v := frameNum
			fn = &v // FrameNumber só no primeiro pacote do access unit
		}
		pkt := rtp.CreatePacketWithDuration(payload, 0, last)
		pkt.Header.Extension = true
		pkt.Header.ExtensionProfile = videoExtProfile
		pkt.Header.ExtensionData = buildWhatsappVideoExt(mediaFrameInfo, fn, tseq)
		tseq++
		packets = append(packets, pkt)
	}

	p.transportSeq = tseq
	p.frameNumber = frameNum + 1
	if idr {
		p.keyframeRequired = false
		p.idrWaitAt = time.Time{}
	}
	relay := p.relay
	p.mu.Unlock()
	locked = false
	for _, pkt := range packets {
		if protected, err := srtp.Protect(pkt); err == nil {
			relay.Broadcast(protected)
		}
	}
}

func (p *Pipeline) requestKeyframeLocked(now time.Time) func() {
	if p.OnKeyframeRequired == nil || (!p.lastKFAt.IsZero() && now.Sub(p.lastKFAt) < time.Second) {
		return nil
	}
	p.lastKFAt = now
	return p.OnKeyframeRequired
}

func hasIDR(nalus [][]byte) bool {
	for _, n := range nalus {
		if len(n) > 0 && n[0]&0x1f == 5 {
			return true
		}
	}
	return false
}

func (p *Pipeline) HandleRelayData(data []byte) {
	p.receiveMu.Lock()
	defer p.receiveMu.Unlock()
	p.mu.Lock()
	if p.srtp == nil || p.depack == nil || len(data) < 12 {
		p.mu.Unlock()
		return
	}
	if media.RTPSsrc(data) == p.selfSsrc {
		p.mu.Unlock()
		return
	}

	pkt, err := p.srtp.Unprotect(data)
	if err != nil {
		p.mu.Unlock()
		p.log.Debug("video srtp unprotect error", "err", err)
		return
	}
	frame, orientationChanged := p.receivePacketLocked(pkt)
	cb := p.OnFrame
	orientationCB, callID, orientation := p.OnOrientation, p.callID, p.peerOrientation
	p.mu.Unlock()

	if orientationChanged && orientationCB != nil {
		orientationCB(callID, orientation)
	}
	if frame != nil && cb != nil {
		cb(frame)
	}
}

func (p *Pipeline) Reset() {
	p.mu.Lock()
	p.resetLocked()
	p.mu.Unlock()
}

func (p *Pipeline) resetLocked() {
	p.rtp = nil
	p.srtp = nil
	p.selfSsrc = 0
	p.depack = nil
	p.frameBuf = nil
	p.firstAUAt = time.Time{}
	p.clockTicks = 0
	p.lastKFAt = time.Time{}
	p.idrWaitAt = time.Time{}
	p.frameNumber = 1
	p.transportSeq = 0
	p.keyframeRequired = true
	p.callID = ""
	p.envelopeCapable = false
	p.peerOrientation = 0
	p.rtpOrientationKnown = false
	p.receive = receiveState{}
	p.receiveSPS, p.receivePPS = nil, nil
}

// BeginCall limpa metadados antes de Setup (Q1/stanza podem chegar antes das chaves).
func (p *Pipeline) BeginCall(callID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.callID != callID {
		p.resetLocked()
		p.callID = callID
	}
}

// EnableEnvelope é idempotente e vale até o fim da chamada, inclusive após rekey.
func (p *Pipeline) EnableEnvelope() {
	p.mu.Lock()
	p.envelopeCapable = true
	p.mu.Unlock()
}

// SetOrientationFallback aplica a stanza até existir CVO RTP válido na chamada.
// Depois disso, ausência de extensão preserva o último CVO, sem regressar à stanza.
// Não chama OnOrientation: o chamador já detém o lock da call e emite o snapshot.
func (p *Pipeline) SetOrientationFallback(orientation int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if orientation >= 0 && orientation <= 3 && !p.rtpOrientationKnown {
		p.peerOrientation = orientation
	}
	return p.peerOrientation
}

// Q1, negociado por texto no DataChannel h264, habilita este envelope binário:
// [0]=0x51, [1]=0x01, [2]=orientação, [3]=flags (bit0=IDR, demais=0),
// [4:8]=sequência da AU uint32 BE (começa em 1, inclui AUs descartadas),
// [8:12]=timestamp RTP uint32 BE, [12:]=AU Annex-B intacta.
// Renderer: para exibir em pé, rotacionar NO SENTIDO HORÁRIO em torno do centro:
// 0 -> 0°, 1 -> 90°, 2 -> 180°, 3 -> 270°. Trocar largura/altura em 1 e 3.
// Derivado de AstraCalls 69e1ef8, pipeline.go:50-78 (CVO = quartos de volta horários).
// Sem Q1, enviar os bytes Annex-B originais, sem prefixo.
func envelopeAU(au []byte, orientation int, idr bool, sequence, timestamp uint32) []byte {
	out := make([]byte, 12+len(au))
	out[0], out[1], out[2] = 0x51, 0x01, byte(orientation)
	if idr {
		out[3] = 1
	}
	binary.BigEndian.PutUint32(out[4:8], sequence)
	binary.BigEndian.PutUint32(out[8:12], timestamp)
	copy(out[12:], au)
	return out
}

func (p *Pipeline) DisableEnvelope() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.envelopeCapable = false
	p.idrWaitAt = time.Time{}
}
