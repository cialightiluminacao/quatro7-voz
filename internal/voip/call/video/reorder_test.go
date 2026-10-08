package video

import (
	"testing"
	"time"
	"wacalls/internal/voip/media"
)

func reorderPacket(seq uint16, ts uint32, size int) *media.RtpPacket {
	return &media.RtpPacket{Header: media.NewRtpHeader(97, seq, ts, 42), Payload: make([]byte, size)}
}

func TestVideoReorderWrapDuplicatesAndDeadline(t *testing.T) {
	var r videoReorder
	now := time.Unix(100, 0)
	r.push(reorderPacket(65534, 1, 10), now)
	if p, ok := r.pop(now); !ok || p.Header.SequenceNumber != 65534 {
		t.Fatal("primeiro pacote")
	}
	r.push(reorderPacket(0, 2, 10), now)
	if ok, _ := r.push(reorderPacket(0, 2, 10), now); ok {
		t.Fatal("duplicata pendente")
	}
	if _, ok := r.pop(now.Add(videoReorderWait - time.Nanosecond)); ok {
		t.Fatal("declarou perda antes da janela")
	}
	r.push(reorderPacket(65535, 2, 10), now.Add(time.Millisecond))
	for _, seq := range []uint16{65535, 0} {
		p, ok := r.pop(now)
		if !ok || p.Header.SequenceNumber != seq {
			t.Fatal("reordenação no rollover")
		}
	}
	if r.missing != 0 || r.bytes != 0 || len(r.pending) != 0 {
		t.Fatal("perda falsa ou buffer retido")
	}
	for i := 0; i < 12; i++ {
		if ok, restart := r.push(reorderPacket(65534, 1, 10), now); ok || restart {
			t.Fatal("duplicata reiniciou")
		}
	}
	r.push(reorderPacket(2, 3, 10), now)
	if p, ok := r.pop(now.Add(videoReorderWait)); !ok || p.Header.SequenceNumber != 2 || r.missing != 1 {
		t.Fatal("deadline não liberou perda verdadeira")
	}
}

func TestVideoReorderBounds(t *testing.T) {
	for _, mode := range []string{"distance", "packets", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			var r videoReorder
			now := time.Unix(100, 0)
			r.push(reorderPacket(0, 1, 1), now)
			r.pop(now)
			switch mode {
			case "distance":
				r.push(reorderPacket(videoReorderPackets+1, 2, 1), now)
			case "packets":
				for i := 0; i < videoReorderPackets; i++ {
					r.push(reorderPacket(uint16(i+2), 2, 1), now)
				}
			case "bytes":
				r.push(reorderPacket(2, 2, videoReorderBytes), now)
			}
			if _, ok := r.pop(now); !ok {
				t.Fatal("limite não liberou fila")
			}
		})
	}
}

func TestVideoReorderOldRelayDoesNotRestart(t *testing.T) {
	var r videoReorder
	now := time.Unix(100, 0)
	r.push(reorderPacket(500, 500, 1), now)
	r.pop(now)
	for seq := uint16(1); seq < 150; seq++ {
		if ok, restart := r.push(reorderPacket(seq, uint32(seq), 1), now); ok || restart {
			t.Fatal("relay velho recuou sequência")
		}
	}
	for seq := uint16(1); seq <= 8; seq++ {
		ok, restart := r.push(reorderPacket(seq, uint32(600+seq), 1), now)
		if seq < 8 && (ok || restart) {
			t.Fatal("reinício prematuro")
		}
		if seq == 8 && (!ok || !restart) {
			t.Fatal("sender reiniciado não recuperou")
		}
	}
	if p, ok := r.pop(now); !ok || p.Header.SequenceNumber != 8 {
		t.Fatal("reinício não drenou")
	}
	p := reorderPacket(100, 1, 1)
	p.Header.Ssrc = 99
	if ok, _ := r.push(p, now); !ok || r.ssrc != 99 || len(r.pending) != 1 {
		t.Fatal("mudança SSRC não isolou fila")
	}
}
