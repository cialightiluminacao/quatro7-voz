package transport

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
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
	channel         mediaChannel
	log             *slog.Logger
	id              string
	stop            <-chan struct{}
	done            chan struct{}
	wake            chan struct{}
	sendMu          sync.Mutex
	priorityWaiting atomic.Int32
	mu              sync.Mutex
	queue           []*videoAccessUnit
	bytes           int
	needIDR         bool
	closed          bool
	drops           uint64
	lastDropLog     time.Time
	onError         func()
}

func newRelayMediaSender(channel mediaChannel, log *slog.Logger, id string, stop <-chan struct{}, onError func()) *relayMediaSender {
	return &relayMediaSender{channel: channel, log: log, id: id, stop: stop,
		done: make(chan struct{}), wake: make(chan struct{}, 1), needIDR: true, onError: onError}
}

func (s *relayMediaSender) sendPriority(data []byte) error {
	s.priorityWaiting.Add(1)
	s.sendMu.Lock()
	s.priorityWaiting.Add(-1)
	defer s.sendMu.Unlock()
	return s.channel.Send(data)
}

func (s *relayMediaSender) enqueue(au *videoAccessUnit) {
	s.mu.Lock()
	select {
	case <-s.stop:
		s.mu.Unlock()
		return
	default:
	}
	if s.closed {
		s.mu.Unlock()
		return
	}
	var dropped []*videoAccessUnit
	reason := "waiting-idr"
	if s.bytes+au.bytes > videoQueueBytes || len(s.queue) >= videoQueueFrames {
		// Descarta todo o GOP pendente. Um IDR novo pode recuperar imediatamente.
		dropped = s.queue
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
	} else {
		s.queue = append(s.queue, au)
		s.bytes += au.bytes
	}
	s.mu.Unlock()
	s.notifyDrops(dropped, reason, time.Now())
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
	for len(s.queue) > 0 {
		au := s.queue[0]
		if au.valid != nil && !au.valid() {
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
	}
	if len(s.queue) == 0 || s.closed || s.priorityWaiting.Load() > 0 || !s.sendMu.TryLock() {
		s.mu.Unlock()
		s.notifyDrops(dropped, "stale-or-dependent", now)
		return
	}
	au := s.queue[0]
	packet := au.packets[au.next]
	var err error
	if s.priorityWaiting.Load() == 0 && s.channel.BufferedAmount()+uint64(len(packet)) <= videoSCTPBytes {
		err = s.channel.Send(packet)
		if err == nil {
			if au.idr {
				s.needIDR = false
			}
			au.next++
			if au.next == len(au.packets) {
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
		}
	}
	s.sendMu.Unlock()
	s.mu.Unlock()
	if err != nil && s.onError != nil {
		s.onError()
	}
	s.notifyDrops(dropped, "stale-or-send-error", now)
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
	for _, sender := range senders {
		sender.enqueue(&videoAccessUnit{packets: packets, idr: idr, at: at, bytes: n, valid: valid, onDrop: onDrop})
	}
}

func (s *relayMediaSender) notifyDrops(dropped []*videoAccessUnit, reason string, now time.Time) {
	for _, au := range dropped {
		s.mu.Lock()
		s.drops++
		count := s.drops
		logDrop := s.lastDropLog.IsZero() || now.Sub(s.lastDropLog) >= time.Second
		if logDrop {
			s.lastDropLog = now
		}
		s.mu.Unlock()
		if logDrop {
			s.log.Info("video outbound dropped", "relay", s.id, "reason", reason,
				"age_ms", now.Sub(au.at).Milliseconds(), "sent_fragments", au.next,
				"drops", count, "sctp_buffered", s.channel.BufferedAmount())
		}
		if au.onDrop != nil {
			au.onDrop()
		}
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
		s.queue, s.bytes = nil, 0
		s.mu.Unlock()
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
