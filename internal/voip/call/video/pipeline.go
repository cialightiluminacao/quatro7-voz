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
	rtpStepSamples = 90000 / 15 // contrato do encoder: 15 Hz, sem jitter da chegada
	// Backpressure global antes de packetizar; o relay aplica idade por AU e
	// um teto menor de vídeo no SCTP, inclusive durante uma AU grande.
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

	sendMu        sync.Mutex // serializa packetização e SRTP de envio fora de mu
	mu            sync.Mutex
	receiveMu     sync.Mutex // serializa recepção e callbacks; nunca adquirido sob mu
	rtp           *media.RtpSession
	srtp          *media.SrtpSession
	srtcp         *media.SrtcpSession
	selfSsrc      uint32
	selfAudioSsrc uint32
	depack        *transport.H264Depacketizer
	frameBuf      []byte
	firstAUAt     time.Time
	clockTicks    uint64
	now           func() time.Time
	afterFunc     func(time.Duration, func()) *time.Timer
	lastKFAt      time.Time
	idrWaitAt     time.Time

	callID                 string
	envelopeCapable        bool
	peerOrientation        int
	rtpOrientationKnown    bool
	receive                receiveState
	receiveSPS, receivePPS []byte
	sendSPS, sendPPS       []byte
	reorder                videoReorder
	receiveTimer           *time.Timer
	receiveEpoch           uint64
	lastPeerKFAt           time.Time
	peerPLICount           uint64
	outboundEpoch          uint64
	outboundIDR            uint64
	rtcpSeen               map[uint16]bool
	diag                   videoDiagnostics
	monitorTimer           *time.Timer
	monitorEpoch           uint64
	// Guard estável do CallManager: timers e pacotes compartilham o lock da
	// troca de bridge. Adquirido antes de receiveMu e mu.
	ReceiveGuard *sync.Mutex

	// Estado da extensão de vídeo do WhatsApp (perfil 0xDEBE) no envio.
	frameNumber      uint16 // nº do frame (por access unit), começa em 1
	transportSeq     uint16 // sequência transport (por pacote)
	keyframeRequired bool   // o primeiro frame enviado precisa ser IDR

	OnFrame func(au []byte)
	// Callbacks rodam sem mu; Setup/Reset podem ser chamados com o lock da call.
	OnOrientation      func(callID string, orientation int)
	OnKeyframeRequired func()
	// REMB autenticado que lista explicitamente o SSRC de vídeo local.
	OnBitrateRequired func(uint64)
	OnDiagnostics     func(string)
}

func New(log *slog.Logger, relay Relay) *Pipeline {
	if log == nil {
		log = slog.Default()
	}
	return &Pipeline{log: log, relay: relay, now: time.Now, afterFunc: time.AfterFunc}
}

func (p *Pipeline) Setup(callID, ourDeviceJid, peerDeviceJid string, sendKM, recvKM core.SrtpKeyingMaterial) error {
	srtp, err := media.NewSrtpSession(sendKM, recvKM, core.SRTPSendAuthTagLen, core.SRTPRecvAuthTagLen)
	if err != nil {
		return err
	}
	srtcp, err := media.NewSrtcpSession(sendKM, recvKM)
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
	p.outboundEpoch++
	// Setup pode repetir com os mesmos masters: não reutilizar índice/IV nem
	// apagar a janela de replay SRTCP dessa chamada.
	srtcp.PreserveUnchangedKeys(p.srtcp)
	p.srtcp = srtcp
	p.selfSsrc = selfSsrc
	p.selfAudioSsrc = media.GenerateSecureSsrc(callID, ourDeviceJid, 0)
	p.rtp = media.NewH264Session(selfSsrc)
	p.depack = &transport.H264Depacketizer{}
	p.receive = receiveState{auSequence: p.receive.auSequence, needIDR: true}
	p.stopReceiveTimerLocked()
	p.reorder = videoReorder{}
	p.frameBuf = nil
	p.frameNumber = 1
	p.transportSeq = 0
	p.keyframeRequired = true
	p.firstAUAt = time.Time{}
	p.clockTicks = 0
	// Os contadores de RTP do SR seguem o novo stream; totais de diagnóstico
	// continuam pertencendo à chamada, inclusive depois de um rekey.
	p.diag.srPackets, p.diag.srOctets = 0, 0
	p.diag.srAt = time.Time{}
	// A ausência de RTP/RTCP também precisa aparecer em INFO. Iniciar apenas
	// no primeiro pacote tornaria uma chamada sem mídia invisível ao monitor.
	p.startMonitorLocked()
	p.mu.Unlock()

	if relay, ok := p.relay.(interface{ SetVideoCallID(string) }); ok {
		relay.SetVideoCallID(callID)
	}
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
	p.mu.Lock()
	at := p.now()
	p.mu.Unlock()
	p.FeedCapturedAt(au, at)
}

// at é a chegada ao motor; inclui espera pelos locks de bridge/packetização.
func (p *Pipeline) FeedCapturedAt(au []byte, at time.Time) {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	p.mu.Lock()
	locked := true
	p.diag.txAUs++
	p.diag.lastCapture = p.now()
	p.startMonitorLocked()
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
		countReason(&p.diag.txDrops, "not-ready-or-empty")
		return
	}
	if len(au) > maxReceiveAUBytes {
		countReason(&p.diag.txDrops, "access-unit-limit")
		p.keyframeRequired = true
		requestKeyframe = p.requestKeyframeLocked(p.now())
		return
	}
	nalus := transport.SplitAnnexB(au)
	if len(nalus) == 0 {
		countReason(&p.diag.txDrops, "invalid-annexb")
		return
	}
	// Uma AU codificada ocupa um slot de 15 Hz, inclusive se for descartada
	// abaixo. Antes do primeiro IDR admitido, o relógio ainda não começou.
	// Chegadas iguais/regredidas e atrasos de IDR não podem virar passos de 1 tick.
	var timestampStep uint32
	if !p.firstAUAt.IsZero() {
		timestampStep = rtpStepSamples
		rtp.AdvanceTimestamp(timestampStep)
		p.clockTicks += uint64(timestampStep)
	}
	idr := hasIDR(nalus)
	haveSPS, havePPS := false, false
	for _, n := range nalus {
		switch n[0] & 31 {
		case 7:
			p.sendSPS = append([]byte(nil), n...)
			haveSPS = true
		case 8:
			p.sendPPS = append([]byte(nil), n...)
			havePPS = true
		}
	}
	// Repetir parameter sets conhecidos em cada IDR, mesmo se o encoder
	// só os tiver entregue na configuração inicial. Nunca duplicar os da AU.
	if idr {
		var prefix [][]byte
		if !haveSPS && len(p.sendSPS) > 0 {
			prefix = append(prefix, p.sendSPS)
		}
		nalus = append(prefix, nalus...)
		if !havePPS && len(p.sendPPS) > 0 {
			// PPS depois do SPS já presente, antes da primeira slice.
			at := len(nalus)
			for i, n := range nalus {
				if kind := n[0] & 31; kind >= 1 && kind <= 5 {
					at = i
					break
				}
			}
			withPPS := make([][]byte, 0, len(nalus)+1)
			withPPS = append(withPPS, nalus[:at]...)
			withPPS = append(withPPS, p.sendPPS)
			nalus = append(withPPS, nalus[at:]...)
		}
	}
	now := p.now()
	if now.Sub(at) >= transport.VideoMaxAge {
		countReason(&p.diag.txDrops, "capture-age")
		p.keyframeRequired = true
		requestKeyframe = p.requestKeyframeLocked(now)
		return
	}
	// Após um drop, repete KF enquanto espera IDR, no máximo uma vez por segundo.
	if p.relay.BufferedAmount() > congestionDropBytes {
		countReason(&p.diag.txDrops, "sctp-buffer")
		if !p.keyframeRequired || p.idrWaitAt.IsZero() {
			p.idrWaitAt = now
		}
		p.keyframeRequired = true
		requestKeyframe = p.requestKeyframeLocked(now)
		return
	}
	// Nunca envia delta dependente de uma AU descartada, mesmo após dois segundos.
	if p.keyframeRequired && !idr {
		countReason(&p.diag.txDrops, "waiting-idr")
		if p.idrWaitAt.IsZero() {
			p.idrWaitAt = now
		}
		requestKeyframe = p.requestKeyframeLocked(now)
		return
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
		countReason(&p.diag.txDrops, "empty-payload")
		return
	}
	if len(packed) > maxReceiveAUBytes {
		countReason(&p.diag.txDrops, "access-unit-limit")
		p.keyframeRequired = true
		requestKeyframe = p.requestKeyframeLocked(now)
		return
	}
	if p.firstAUAt.IsZero() {
		p.firstAUAt = at
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
	p.logCapturedH264Locked(idr, len(au), len(packed), len(packets), packets[0].Header.Timestamp, timestampStep)

	p.transportSeq = tseq
	p.frameNumber = frameNum + 1
	if idr {
		p.keyframeRequired = false
		p.idrWaitAt = time.Time{}
		p.outboundIDR++
	}
	relay := p.relay
	epoch := p.outboundEpoch
	idrGeneration := p.outboundIDR
	p.mu.Unlock()
	locked = false
	protectedAU := make([][]byte, 0, len(packets))
	for _, pkt := range packets {
		protected, err := srtp.Protect(pkt)
		if err != nil {
			p.mu.Lock()
			countReason(&p.diag.txDrops, "srtp-protect")
			p.mu.Unlock()
			p.outboundDropped(epoch, idrGeneration)
			return
		}
		protectedAU = append(protectedAU, protected)
	}
	p.mu.Lock()
	if p.outboundEpoch != epoch || p.rtp == nil {
		p.mu.Unlock()
		return
	}
	p.diag.txSubmitted++
	p.diag.txBytes += uint64(len(au))
	firstRTP := p.diag.srPackets == 0
	for _, pkt := range packets {
		p.diag.srPackets++
		p.diag.srOctets += uint32(len(pkt.Payload))
		p.diag.srTimestamp = pkt.Header.Timestamp
	}
	// SR e RTP compartilham a mesma linha do tempo nominal. Vincular o último
	// timestamp à chegada real reintroduziria jitter/deriva no mapeamento RTP/NTP.
	nominalElapsed := time.Duration(p.clockTicks/90000)*time.Second + time.Duration(p.clockTicks%90000)*time.Second/90000
	p.diag.srRtpAt = p.firstAUAt.Add(nominalElapsed)
	// Um monitor que começou só para recepção pode estar aguardando 5 s.
	// Reagenda para começar os SR de envio em 1,5 s.
	if firstRTP {
		p.stopMonitorLocked()
		p.startMonitorLocked()
	}
	p.mu.Unlock()
	if sender, ok := relay.(interface {
		BroadcastVideoAU([][]byte, bool, time.Time, func() bool, func())
	}); ok {
		sender.BroadcastVideoAU(protectedAU, idr, at, func() bool {
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.outboundEpoch == epoch && p.rtp != nil
		}, func() { p.outboundDropped(epoch, idrGeneration) })
	} else {
		for _, packet := range protectedAU {
			relay.Broadcast(packet)
		}
	}
}

func (p *Pipeline) outboundDropped(epoch, idrGeneration uint64) {
	p.mu.Lock()
	if p.outboundEpoch != epoch || p.rtp == nil {
		p.mu.Unlock()
		return
	}
	// Um relay lento não invalida um IDR mais novo já recebido do browser.
	if p.outboundIDR == idrGeneration {
		p.keyframeRequired = true
	}
	request := p.requestKeyframeLocked(p.now())
	p.mu.Unlock()
	if request != nil {
		request()
	}
}

func (p *Pipeline) requestKeyframeLocked(now time.Time) func() {
	if p.OnKeyframeRequired == nil || (!p.lastKFAt.IsZero() && now.Sub(p.lastKFAt) < time.Second) {
		return nil
	}
	p.lastKFAt = now
	p.diag.browserKF++
	p.log.Debug("video browser keyframe requested", "call_id", p.callID)
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
	unlock := p.lockReceive()
	defer unlock()
	p.mu.Lock()
	p.diag.rxPackets++
	p.startMonitorLocked()
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
		p.diag.rxAuthRejected++
		p.mu.Unlock()
		p.log.Debug("video srtp unprotect error", "err", err)
		return
	}
	p.diag.lastPacket = p.now()
	accepted, restarted := p.reorder.push(pkt, p.now())
	if restarted {
		p.discardReceiveAULocked("sequence-restart")
		p.receive.seqKnown, p.receive.haveTimestamp, p.receive.active = false, false, false
	}
	if accepted {
		p.drainReceiveLocked(p.now())
	}
	p.sendPeerKeyframeLocked()
	p.scheduleReceiveLocked()
	p.mu.Unlock()
}

func (p *Pipeline) Reset() {
	p.mu.Lock()
	p.resetLocked()
	p.mu.Unlock()
}

func (p *Pipeline) resetLocked() {
	p.outboundEpoch++
	p.outboundIDR = 0
	p.rtp = nil
	p.srtp = nil
	p.srtcp = nil
	p.selfSsrc = 0
	p.selfAudioSsrc = 0
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
	p.stopReceiveTimerLocked()
	p.reorder = videoReorder{}
	p.lastPeerKFAt = time.Time{}
	p.peerPLICount = 0
	p.rtcpSeen = nil
	p.receiveSPS, p.receivePPS = nil, nil
	p.sendSPS, p.sendPPS = nil, nil
	p.stopMonitorLocked()
	p.diag = videoDiagnostics{}
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
	p.outboundEpoch++ // cancela vídeo pendente da bridge substituída
	p.keyframeRequired = true
	p.idrWaitAt = time.Time{}
}
