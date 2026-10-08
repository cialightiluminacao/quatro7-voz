package call

import (
	"time"

	"wacalls/internal/voip/media"
)

const (
	peerReorderWait    = 60 * time.Millisecond
	peerReorderPackets = 3
	peerResyncLate     = 8
)

type peerAudioPacket struct {
	header    *media.RtpHeader
	payload   []byte // já autenticado e decifrado
	at        time.Time
	reordered bool
	missing   uint16 // posições de sequência perdidas antes deste pacote
}

// Janela por sequência, independente do timestamp e do decoder. Requer m.mu.
// int16(seq-next) mantém a ordem inclusive no wrap 65535->0.
type peerAudioReorder struct {
	set             bool
	next            uint16
	pending         []peerAudioPacket
	seen            [64]uint16
	seenSet         [64]bool
	ssrc            uint32
	consecutiveLate int
}

func (r *peerAudioReorder) push(header *media.RtpHeader, payload []byte, now time.Time) (duplicate, late, resynced bool) {
	seq := header.SequenceNumber
	if !r.set || r.ssrc != header.Ssrc {
		resynced = r.set
		*r = peerAudioReorder{set: true, next: seq, ssrc: header.Ssrc}
	}
	slot := int(seq) % len(r.seen)
	if r.seenSet[slot] && r.seen[slot] == seq {
		return true, false, resynced
	}
	delta := int16(seq - r.next)
	if delta < 0 {
		r.consecutiveLate++
		if r.consecutiveLate < peerResyncLate {
			r.seen[slot], r.seenSet[slot] = seq, true
			return false, true, resynced
		}
		// Reinício do sender/relay: oito atrasados distintos, sem áudio em ordem.
		*r = peerAudioReorder{set: true, next: seq, ssrc: header.Ssrc}
		delta, resynced = 0, true
	}
	r.consecutiveLate = 0
	r.seen[slot], r.seenSet[slot] = seq, true
	p := peerAudioPacket{header: header, payload: append([]byte(nil), payload...), at: now, reordered: delta > 0}
	i := 0
	for i < len(r.pending) && int16(r.pending[i].header.SequenceNumber-r.next) < delta {
		i++
	}
	r.pending = append(r.pending, peerAudioPacket{})
	copy(r.pending[i+1:], r.pending[i:])
	r.pending[i] = p
	return false, false, resynced
}

func (r *peerAudioReorder) deadline() time.Time {
	var oldest time.Time
	for _, p := range r.pending {
		if oldest.IsZero() || p.at.Before(oldest) {
			oldest = p.at
		}
	}
	return oldest.Add(peerReorderWait)
}

func (r *peerAudioReorder) pop(now time.Time) (peerAudioPacket, bool) {
	if len(r.pending) == 0 {
		return peerAudioPacket{}, false
	}
	p := r.pending[0]
	seq := p.header.SequenceNumber
	if seq != r.next && int16(seq-r.next) < peerReorderPackets && len(r.pending) < peerReorderPackets && now.Before(r.deadline()) {
		return peerAudioPacket{}, false
	}
	p.missing = seq - r.next
	r.next = seq + 1
	r.pending = r.pending[1:]
	return p, true
}

// Retomada mantém a sequência/histórico, mas não reproduz pacotes da espera.
func (r *peerAudioReorder) discardPending() {
	if n := len(r.pending); n > 0 {
		r.next = r.pending[n-1].header.SequenceNumber + 1
	}
	r.pending = nil
}
