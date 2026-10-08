package video

import (
	"time"
	"wacalls/internal/voip/media"
)

const (
	videoReorderWait    = 60 * time.Millisecond
	videoReorderPackets = 64
	videoReorderBytes   = 256 << 10
)

type queuedVideoPacket struct {
	packet *media.RtpPacket // autenticado, dono dos bytes
	at     time.Time
}

// Janela limitada por tempo, pacotes e bytes; int16 mantém ordem no rollover.
// Duplicatas de outro relay não avançam sequência nem contam como reinício.
type videoReorder struct {
	set                                  bool
	ssrc                                 uint32
	next                                 uint16
	lastTimestamp                        uint32
	pending                              []queuedVideoPacket
	bytes                                int
	lateRun                              int
	lastLate                             uint16
	seen                                 [128]uint16
	seenSet                              [128]bool
	duplicates, reordered, late, missing uint64
}

func (r *videoReorder) push(pkt *media.RtpPacket, now time.Time) (accepted, restarted bool) {
	h := pkt.Header
	if !r.set || r.ssrc != h.Ssrc {
		*r = videoReorder{set: true, ssrc: h.Ssrc, next: h.SequenceNumber}
	}
	seq := h.SequenceNumber
	i := int(seq) % len(r.seen)
	if r.seenSet[i] && r.seen[i] == seq {
		r.duplicates++
		return false, false
	}
	d := int16(seq - r.next)
	if d < 0 {
		r.late++
		// Só ressinc com oito sequências distintas consecutivas e timestamp
		// MAIS NOVO que o entregue. Um relay atrasado não reinicia o stream.
		if int32(h.Timestamp-r.lastTimestamp) > 0 {
			if r.lateRun > 0 && seq == r.lastLate+1 {
				r.lateRun++
			} else {
				r.lateRun = 1
			}
			r.lastLate = seq
		} else {
			r.lateRun = 0
		}
		if r.lateRun < 8 {
			r.seen[i], r.seenSet[i] = seq, true
			return false, false
		}
		*r = videoReorder{set: true, ssrc: h.Ssrc, next: seq}
		d, restarted = 0, true
	}
	r.lateRun = 0
	r.seen[i], r.seenSet[i] = seq, true
	if d > 0 {
		r.reordered++
	}
	j := 0
	for j < len(r.pending) && int16(r.pending[j].packet.Header.SequenceNumber-r.next) < d {
		j++
	}
	r.pending = append(r.pending, queuedVideoPacket{})
	copy(r.pending[j+1:], r.pending[j:])
	r.pending[j] = queuedVideoPacket{packet: pkt, at: now}
	r.bytes += pkt.Header.Size() + len(pkt.Payload)
	return true, restarted
}

func (r *videoReorder) deadline() time.Time {
	var oldest time.Time
	for _, p := range r.pending {
		if oldest.IsZero() || p.at.Before(oldest) {
			oldest = p.at
		}
	}
	return oldest.Add(videoReorderWait)
}

func (r *videoReorder) pop(now time.Time) (*media.RtpPacket, bool) {
	if len(r.pending) == 0 {
		return nil, false
	}
	p := r.pending[0].packet
	d := uint16(p.Header.SequenceNumber - r.next)
	if d != 0 && d < videoReorderPackets && len(r.pending) < videoReorderPackets && r.bytes < videoReorderBytes && now.Before(r.deadline()) {
		return nil, false
	}
	r.missing += uint64(d)
	r.next = p.Header.SequenceNumber + 1
	r.lastTimestamp = p.Header.Timestamp
	r.bytes -= p.Header.Size() + len(p.Payload)
	r.pending[0] = queuedVideoPacket{}
	r.pending = r.pending[1:]
	return p, true
}
