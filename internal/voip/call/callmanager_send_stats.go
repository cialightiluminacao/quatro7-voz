package call

import "time"

// SendStats contém os deltas do período do log [voz-drift]. Jitter é o desvio
// absoluto do intervalo REAL entre envios em relação aos 60ms esperados.
type SendStats struct {
	CaptureFrames         int64
	CaptureUnderruns      int64
	CaptureDroppedSamples int64 // 16kHz
	SentFrames            int64
	JitterIntervals       int64
	JitterTotal           time.Duration
	JitterMax             time.Duration
	EncodeMax             time.Duration
	Errors                int64 // encode + SRTP + erros de envio nos relays
}

func (m *CallManager) noteSendLocked(now time.Time) {
	m.sendStatsMu.Lock()
	defer m.sendStatsMu.Unlock()
	if !m.lastSend.IsZero() {
		jitter := now.Sub(m.lastSend) - 60*time.Millisecond
		if jitter < 0 {
			jitter = -jitter
		}
		m.sendStats.JitterIntervals++
		m.sendStats.JitterTotal += jitter
		m.sendStats.JitterMax = max(m.sendStats.JitterMax, jitter)
	}
	m.lastSend = now
	m.sendStats.SentFrames++
}

func (m *CallManager) noteSendError() {
	m.sendStatsMu.Lock()
	m.sendStats.Errors++
	m.sendStatsMu.Unlock()
}

// DrainSendStats lê e zera apenas os contadores do período, preservando o último
// instante de envio para medir também o intervalo na fronteira entre períodos.
func (m *CallManager) DrainSendStats() SendStats {
	// O playout pode drenar enquanto OnStateChange (sob mu) espera seu término.
	// Por isso o leitor usa só sendStatsMu, nunca mu.
	m.sendStatsMu.Lock()
	stats := m.sendStats
	m.sendStats = SendStats{}
	m.sendStatsMu.Unlock()
	if relay, ok := m.relay.(interface{ DrainSendErrors() int64 }); ok {
		stats.Errors += relay.DrainSendErrors()
	}
	return stats
}
