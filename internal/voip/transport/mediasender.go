package transport

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

const (
	VideoMaxAge      = 400 * time.Millisecond
	videoQueueBytes  = 256 << 10
	videoQueueFrames = 8
	// Inclui dados em voo esperando ACK: 8 KiB permitem ~500 kbit/s com RTT
	// de 100 ms. A maior parte da IDR continua na fila descartável, enquanto
	// áudio tem prioridade na admissão de cada novo fragmento.
	videoSCTPBytes    = 8 << 10
	videoSendInterval = 2 * time.Millisecond
)

type mediaChannel interface {
	Send([]byte) error
	BufferedAmount() uint64
}

type videoAccessUnit struct {
	packets     [][]byte // SRTP já protegido; propriedade transferida ao sender
	idr         bool
	at          time.Time // chegada ao motor, antes de adquirir locks/packetizar
	onDrop      func()
	valid       func() bool // call/rekey/bridge epoch; cancelamento não solicita KF
	next, bytes int
}

// Um sender por relay: nenhuma espera por congestionamento mantém o lock de
// envio. Áudio/controle entram diretamente; vídeo só libera um fragmento por tick.
type relayMediaSender struct {
	channel                                mediaChannel
	log                                    *slog.Logger
	id                                     string
	callID                                 string
	stop                                   <-chan struct{}
	done                                   chan struct{}
	wake                                   chan struct{}
	sendMu                                 sync.Mutex
	priorityWaiting                        atomic.Int32
	mu                                     sync.Mutex
	queue                                  []*videoAccessUnit
	bytes                                  int
	needIDR                                bool
	closed                                 bool
	drops                                  uint64
	partialDrops, maxAUBytes               uint64
	lastDropLog                            time.Time
	dropReasons                            map[string]uint64
	ausIn, ausSent, packetsSent, wireBytes uint64
	bufferWaits, cancellations             uint64
	maxBuffered                            uint64
	rtcpSent, rtcpErrors                   atomic.Uint64
	pliSent                                atomic.Uint64
	rxVideo, rxRTCP                        atomic.Uint64
	lastRXVideo, lastRXRTCP                atomic.Int64 // Unix ms; zero antes de receber
	lastRTCPLog                            atomic.Int64
	onError                                func()
	rtpSSRC, rtpPackets, rtpOctets         uint32
}

// A origem de RTP/RTCP é conhecida aqui, antes da autenticação/deduplicação
// compartilhada no pipeline. As idades identificam qual relay parou de receber.
func (s *relayMediaSender) observeReceive(data []byte, now time.Time) {
	if len(data) < 8 || data[0]>>6 != 2 {
		return
	}
	if media.IsRTCPPacket(data) {
		s.rxRTCP.Add(1)
		s.lastRXRTCP.Store(now.UnixMilli())
	} else if data[1]&0x7f == core.PayloadTypeWhatsAppH264 && len(data) >= 12 {
		s.rxVideo.Add(1)
		s.lastRXVideo.Store(now.UnixMilli())
	}
}

func relayReceiveAge(now time.Time, at int64) int64 {
	if at == 0 {
		return -1
	}
	return now.UnixMilli() - at
}

func newRelayMediaSender(channel mediaChannel, log *slog.Logger, id string, stop <-chan struct{}, onError func()) *relayMediaSender {
	return &relayMediaSender{channel: channel, log: log, id: id, stop: stop,
		done: make(chan struct{}), wake: make(chan struct{}, 1), needIDR: true, onError: onError}
}

func (s *relayMediaSender) sendPriority(data []byte) error {
	s.priorityWaiting.Add(1)
	s.sendMu.Lock()
	s.priorityWaiting.Add(-1)
	err := s.channel.Send(data)
	s.sendMu.Unlock()
	if len(data) >= 8 && data[0]>>6 == 2 && data[1] >= 192 && data[1] <= 223 {
		if err == nil {
			s.rtcpSent.Add(1)
			if data[1] == 201 {
				s.pliSent.Add(1)
			} // RR+SDES+PLI do pipeline
		} else {
			s.rtcpErrors.Add(1)
		}
		now := time.Now().UnixNano()
		last := s.lastRTCPLog.Load()
		if (last == 0 || now-last >= int64(5*time.Second)) && s.lastRTCPLog.CompareAndSwap(last, now) {
			s.log.Info("video RTCP relay send", "call_id", s.callID, "relay", s.id, "pt", data[1], "bytes", len(data), "sent", s.rtcpSent.Load(), "pli_sent", s.pliSent.Load(), "errors", s.rtcpErrors.Load(), "err", err)
		}
	}
	return err
}

func (s *relayMediaSender) enqueue(au *videoAccessUnit) {
	s.mu.Lock()
	s.ausIn++
	if uint64(au.bytes) > s.maxAUBytes {
		s.maxAUBytes = uint64(au.bytes)
	}
	select {
	case <-s.stop:
		s.mu.Unlock()
		s.notifyDrops([]*videoAccessUnit{au}, "relay-closed", time.Now())
		return
	default:
	}
	if s.closed {
		s.mu.Unlock()
		s.notifyDrops([]*videoAccessUnit{au}, "relay-closed", time.Now())
		return
	}
	var dropped []*videoAccessUnit
	var reasons []string
	reason := "waiting-idr"
	if s.bytes+au.bytes > videoQueueBytes || len(s.queue) >= videoQueueFrames {
		// Descarta todo o GOP pendente. Um IDR novo pode recuperar imediatamente.
		dropped = s.queue
		for range dropped {
			reasons = append(reasons, "queue-limit")
		}
		s.queue, s.bytes, s.needIDR = nil, 0, true
		reason = "queue-limit"
	}
	canReference := !s.needIDR || au.idr
	if !canReference {
		// Deltas posteriores a um IDR pendente podem aguardar na fila. Deltas
		// anteriores continuam bloqueadas até esse IDR ser efetivamente enviado.
		for _, pending := range s.queue {
			if pending.idr {
				canReference = true
				break
			}
		}
	}
	if !canReference || au.bytes > videoQueueBytes {
		dropped = append(dropped, au)
		if au.bytes > videoQueueBytes {
			reason = "au-too-large"
			s.needIDR = true
		}
		reasons = append(reasons, reason)
	} else {
		s.queue = append(s.queue, au)
		s.bytes += au.bytes
	}
	s.mu.Unlock()
	for i, au := range dropped {
		s.notifyDrops([]*videoAccessUnit{au}, reasons[i], time.Now())
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// step também é usado com relógio determinístico nos testes. Após perder uma AU,
// descarta as deltas dependentes, inclusive se a AU expirou entre fragmentos.
func (s *relayMediaSender) step(now time.Time) {
	s.mu.Lock()
	var dropped []*videoAccessUnit
	var reasons []string
	for len(s.queue) > 0 {
		au := s.queue[0]
		if au.valid != nil && !au.valid() {
			s.cancellations++
			s.queue[0] = nil
			s.queue = s.queue[1:]
			s.bytes -= au.bytes
			s.needIDR = true
			continue
		}
		if now.Sub(au.at) < VideoMaxAge && (!s.needIDR || au.idr) {
			break
		}
		s.queue[0] = nil
		s.queue = s.queue[1:]
		s.bytes -= au.bytes
		s.needIDR = true
		dropped = append(dropped, au)
		reason := "waiting-idr"
		if now.Sub(au.at) >= VideoMaxAge {
			reason = "age"
		}
		reasons = append(reasons, reason)
	}
	if len(s.queue) == 0 || s.closed || s.priorityWaiting.Load() > 0 || !s.sendMu.TryLock() {
		s.mu.Unlock()
		for i, au := range dropped {
			s.notifyDrops([]*videoAccessUnit{au}, reasons[i], now)
		}
		return
	}
	au := s.queue[0]
	packet := au.packets[au.next]
	var err error
	buffered := s.channel.BufferedAmount()
	if buffered > s.maxBuffered {
		s.maxBuffered = buffered
	}
	if s.priorityWaiting.Load() == 0 && buffered+uint64(len(packet)) <= videoSCTPBytes {
		err = s.channel.Send(packet)
		if err == nil {
			s.packetsSent++
			s.wireBytes += uint64(len(packet))
			// SR conta somente RTP admitido pelo Send, não AUs enfileiradas
			// que podem expirar. Exclui cabeçalho RTP e tag SRTP dos octetos.
			if h, decodeErr := media.DecodeRtpHeader(packet); decodeErr == nil && h.PayloadType == core.PayloadTypeWhatsAppH264 && len(packet) > h.Size()+core.SRTPSendAuthTagLen {
				if s.rtpSSRC != h.Ssrc {
					s.rtpSSRC, s.rtpPackets, s.rtpOctets = h.Ssrc, 0, 0
				}
				s.rtpPackets++
				s.rtpOctets += uint32(len(packet) - h.Size() - core.SRTPSendAuthTagLen)
			}
			if au.idr {
				s.needIDR = false
			}
			au.next++
			if au.next == len(au.packets) {
				s.ausSent++
				s.queue[0] = nil
				s.queue = s.queue[1:]
				s.bytes -= au.bytes
			}
		} else {
			s.queue[0] = nil
			s.queue = s.queue[1:]
			s.bytes -= au.bytes
			s.needIDR = true
			dropped = append(dropped, au)
			reasons = append(reasons, "send-error")
		}
	} else {
		s.bufferWaits++
	}
	s.sendMu.Unlock()
	s.mu.Unlock()
	if err != nil && s.onError != nil {
		s.onError()
	}
	for i, au := range dropped {
		s.notifyDrops([]*videoAccessUnit{au}, reasons[i], now)
	}
}

// O mesmo RTP pode sair por dois relays: usa o destino com mais pacotes
// enviados, sem somar duplicatas no Sender Report.
func (m *SctpRelayManager) VideoSenderStats(ssrc uint32) (packets, octets uint32) {
	m.mu.Lock()
	var senders []*relayMediaSender
	for _, c := range m.connections {
		if c.state == relayStateOpen && c.sender != nil {
			senders = append(senders, c.sender)
		}
	}
	m.mu.Unlock()
	for _, s := range senders {
		s.mu.Lock()
		if s.rtpSSRC == ssrc && s.rtpPackets > packets {
			packets, octets = s.rtpPackets, s.rtpOctets
		}
		s.mu.Unlock()
	}
	return
}

// BroadcastVideoAU transfere cada AU inteira à fila descartável de cada relay.
// Os bytes SRTP são imutáveis e podem ser compartilhados entre os relays.
func (m *SctpRelayManager) BroadcastVideoAU(packets [][]byte, idr bool, at time.Time, valid func() bool, onDrop func()) {
	if len(packets) == 0 {
		return
	}
	n := 0
	for _, packet := range packets {
		n += len(packet)
	}
	m.mu.Lock()
	var senders []*relayMediaSender
	for _, c := range m.connections {
		if c.state == relayStateOpen && c.sender != nil {
			senders = append(senders, c.sender)
		}
	}
	m.mu.Unlock()
	if len(senders) == 0 {
		if onDrop != nil {
			onDrop()
		}
		return
	}
	// Os relays carregam cópias da mesma AU. Um destino lento descarta seu
	// próprio GOP, sem bloquear o pipeline/relay saudável que entregou a AU.
	// A recuperação global só é necessária se TODOS descartarem essa AU.
	var failed atomic.Int32
	allDropped := func() {
		if failed.Add(1) == int32(len(senders)) && onDrop != nil {
			onDrop()
		}
	}
	for _, sender := range senders {
		sender.enqueue(&videoAccessUnit{packets: packets, idr: idr, at: at, bytes: n, valid: valid, onDrop: allDropped})
	}
}

func (s *relayMediaSender) notifyDrops(dropped []*videoAccessUnit, reason string, now time.Time) {
	for _, au := range dropped {
		s.mu.Lock()
		s.drops++
		if au.next > 0 {
			s.partialDrops++
		}
		if s.dropReasons == nil {
			s.dropReasons = make(map[string]uint64)
		}
		s.dropReasons[reason]++
		count := s.drops
		logDrop := s.lastDropLog.IsZero() || now.Sub(s.lastDropLog) >= 5*time.Second
		if logDrop {
			s.lastDropLog = now
		}
		s.mu.Unlock()
		if logDrop {
			s.log.Info("video outbound dropped", "call_id", s.callID, "relay", s.id, "reason", reason,
				"age_ms", now.Sub(au.at).Milliseconds(), "sent_fragments", au.next,
				"drops", count, "sctp_buffered", s.channel.BufferedAmount())
		}
		if au.onDrop != nil {
			au.onDrop()
		}
	}
}

// Chamado pelo monitor por chamada a cada 5 s, fora do lock do pipeline.
func (m *SctpRelayManager) LogVideoDiagnostics(callID string) {
	m.mu.Lock()
	var senders []*relayMediaSender
	for _, c := range m.connections {
		if c.state == relayStateOpen && c.sender != nil {
			senders = append(senders, c.sender)
		}
	}
	m.mu.Unlock()
	now := time.Now()
	for _, s := range senders {
		if s.callID != "" && s.callID != callID {
			continue
		}
		s.mu.Lock()
		s.log.Info("video relay diagnostics", "call_id", callID, "relay", s.id,
			"tx_aus_in", s.ausIn, "tx_aus_sent", s.ausSent, "tx_packets_sent", s.packetsSent,
			"tx_wire_bytes", s.wireBytes, "tx_drops", s.drops, "tx_drop_reasons", s.dropReasons,
			"tx_partial_drops", s.partialDrops, "tx_max_au_bytes", s.maxAUBytes,
			"tx_cancelled", s.cancellations, "queue_aus", len(s.queue), "queue_bytes", s.bytes,
			"need_idr", s.needIDR, "sctp_buffered", s.channel.BufferedAmount(), "sctp_max_buffered", s.maxBuffered,
			"sctp_limit", videoSCTPBytes, "pacing_ms", videoSendInterval.Milliseconds(),
			"buffer_waits", s.bufferWaits, "rtcp_sent", s.rtcpSent.Load(), "pli_sent", s.pliSent.Load(), "rtcp_errors", s.rtcpErrors.Load(),
			"rx_video_packets", s.rxVideo.Load(), "rx_rtcp_packets", s.rxRTCP.Load(),
			"rx_last_video_ms", relayReceiveAge(now, s.lastRXVideo.Load()), "rx_last_rtcp_ms", relayReceiveAge(now, s.lastRXRTCP.Load()))
		s.maxBuffered = 0
		s.maxAUBytes = 0
		s.mu.Unlock()
	}
}

func (s *relayMediaSender) run() {
	defer close(s.done)
	var ticker *time.Ticker
	var ticks <-chan time.Time
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	defer func() {
		s.mu.Lock()
		s.closed = true
		pending := s.queue
		s.queue, s.bytes = nil, 0
		s.mu.Unlock()
		s.notifyDrops(pending, "relay-closed", time.Now())
	}()
	for {
		select {
		case <-s.stop:
			return
		case <-s.wake:
			// O wake só ativa a fila; o tick mantém o teto de uma rajada/2 ms.
		case <-ticks:
			s.step(time.Now()) // timestamp do tick pode estar atrasado sob carga
		}
		s.mu.Lock()
		pending := len(s.queue) > 0
		s.mu.Unlock()
		if pending && ticker == nil {
			ticker = time.NewTicker(videoSendInterval)
			ticks = ticker.C
		} else if !pending && ticker != nil {
			ticker.Stop()
			ticker, ticks = nil, nil
		}
	}
}
