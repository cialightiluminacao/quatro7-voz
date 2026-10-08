package call

import (
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

type orderedAudioCodec struct {
	guardedTestCodec
	order []int
}

func (c *orderedAudioCodec) Decode(payload []byte) ([]float32, error) {
	pcm, err := c.guardedTestCodec.Decode(payload)
	value := -1 // PLC tem amostras distinguíveis de zeros artificiais
	if len(payload) == 2 {
		value = int(payload[0]) | int(payload[1])<<8
	}
	c.order = append(c.order, value)
	for i := range pcm {
		pcm[i] = float32(value)
	}
	return pcm, err
}

func orderedAudioPacket(t *testing.T, tx *media.SrtpSession, seq uint16, ts uint32) []byte {
	t.Helper()
	p, err := tx.Protect(&media.RtpPacket{
		Header:  media.NewRtpHeader(core.PayloadTypeWhatsAppOpus, seq, ts, 20),
		Payload: []byte{byte(seq), byte(seq >> 8)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func expireAudioWindow(m *CallManager) {
	m.audioReceiveMu.Lock()
	defer m.audioReceiveMu.Unlock()
	m.mu.Lock()
	pcm := m.drainPeerAudioLocked(time.Now().Add(peerReorderWait))
	m.schedulePeerAudioLocked()
	m.mu.Unlock()
	m.deliverPeerAudio(pcm)
}

func TestAudioSequenceReorderBeforeDecode(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		start                 uint16
		arrival               []int
		duplicates, reordered int64
	}{
		{name: "swap", arrival: []int{0, 2, 1, 3}, reordered: 1},
		{name: "duplicate queued", arrival: []int{0, 2, 2, 1, 3}, duplicates: 1, reordered: 1},
		{name: "reverse three", arrival: []int{0, 3, 2, 1}, reordered: 2},
		{name: "16 bit wrap and duplicate", start: 65534, arrival: []int{0, 2, 1, 3, 3}, duplicates: 1, reordered: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			codec := &orderedAudioCodec{}
			m, tx := audioTestManager(t, codec)
			defer m.cleanupMedia()
			var delivered []int
			m.OnPeerAudio = func(pcm []float32) {
				for i := 0; i < len(pcm); i += frame16 {
					delivered = append(delivered, int(pcm[i]))
				}
			}
			packets := make([][]byte, 4)
			var want []int
			for i := range packets {
				seq := tc.start + uint16(i)
				// Timestamp também cruza 32 bits no teste de wrap.
				packets[i] = orderedAudioPacket(t, tx, seq, uint32(0xfffffc00)+uint32(i*frame16))
				want = append(want, int(seq))
			}
			for _, i := range tc.arrival {
				m.handleAudioRelayData(packets[i])
			}
			if !slices.Equal(codec.order, want) || !slices.Equal(delivered, want) || m.PeerDuplicates() != tc.duplicates || m.PeerReordered() != tc.reordered || codec.decodes.Load() != 4 {
				t.Fatalf("decode=%v PCM=%v dup=%d reordered=%d quero=%v", codec.order, delivered, m.PeerDuplicates(), m.PeerReordered(), want)
			}
		})
	}
}

func TestAudioLossUsesBoundedPLCBeforeNextFrame(t *testing.T) {
	for _, tc := range []struct {
		name string
		seq  uint16
		ts   uint32
		plc  int
	}{
		{"one missing", 2, 2 * frame16, 1},
		{"three missing", 4, 4 * frame16, 3},
		{"four missing no padding", 5, 5 * frame16, 0},
		{"large timestamp jump", 1, 160000, 0},
		{"DTX timestamp gap without packet loss", 1, 2 * frame16, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			codec := &orderedAudioCodec{}
			m, tx := audioTestManager(t, codec)
			defer m.cleanupMedia()
			var out []float32
			m.OnPeerAudio = func(pcm []float32) { out = append(out, pcm...) }
			first := orderedAudioPacket(t, tx, 0, 0)
			missing := orderedAudioPacket(t, tx, 1, frame16)
			m.handleAudioRelayData(first)
			m.handleAudioRelayData(orderedAudioPacket(t, tx, tc.seq, tc.ts))
			expireAudioWindow(m)
			want := []int{0}
			for range tc.plc {
				want = append(want, -1)
			}
			want = append(want, int(tc.seq))
			if !slices.Equal(codec.order, want) || len(out) != (2+tc.plc)*frame16 {
				t.Fatalf("decode=%v amostras=%d quero=%v", codec.order, len(out), want)
			}
			if tc.seq != 1 {
				m.handleAudioRelayData(missing)
				if !slices.Equal(codec.order, want) || m.PeerLateDrops() != 1 {
					t.Fatal("pacote tardio tocou decoder")
				}
			}
		})
	}
}

func TestAudioReorderDeadlineReleasesWithoutAnotherPacket(t *testing.T) {
	codec := &orderedAudioCodec{}
	m, tx := audioTestManager(t, codec)
	defer m.cleanupMedia()
	delivered := make(chan int, 2)
	m.OnPeerAudio = func(pcm []float32) { delivered <- len(pcm) }
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 0, 0))
	<-delivered
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 2, 2*frame16))
	m.audioReceiveMu.Lock()
	if codec.decodes.Load() != 1 {
		t.Error("não esperou a janela de reordenação")
	}
	m.audioReceiveMu.Unlock()
	select {
	case n := <-delivered:
		if n != 2*frame16 {
			t.Fatalf("PLC+frame=%d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("timer não liberou frame retido")
	}
}

func TestAudioResumeDiscardsHeldPacketsAndResetsAlignment(t *testing.T) {
	codec := &orderedAudioCodec{}
	m, tx := audioTestManager(t, codec)
	defer m.cleanupMedia()
	m.currentCall.StateData.State = core.CallStateActive
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 0, 0))
	if err := m.Hold(false); err != nil {
		t.Fatal(err)
	}
	held := orderedAudioPacket(t, tx, 2, 2*frame16)
	m.handleAudioRelayData(held)
	reset := false
	m.OnPeerAudioReset = func() { reset = !m.audioTimelineSet && len(m.audioReorder.pending) == 0 }
	if err := m.Resume(); err != nil {
		t.Fatal(err)
	}
	if !reset {
		t.Fatal("retomada não limpou fila/alinhamento")
	}
	var delivered []float32
	m.OnPeerAudio = func(pcm []float32) { delivered = append(delivered, pcm...) }
	m.handleAudioRelayData(held) // segundo relay tenta entregar o pacote da espera
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 3, 3*frame16))
	if !slices.Equal(codec.order, []int{0, 3}) || len(delivered) != frame16 || delivered[0] != 3 {
		t.Fatalf("decode=%v PCM=%v", codec.order, len(delivered))
	}
}

func TestAudioCleanupInvalidatesReorderTimer(t *testing.T) {
	codec := &orderedAudioCodec{}
	m, tx := audioTestManager(t, codec)
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 0, 0))
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 2, 2*frame16))
	m.cleanupMedia()
	time.Sleep(2 * peerReorderWait)
	if codec.decodes.Load() != 1 || codec.violations.Load() != 0 {
		t.Fatal("timer usou decoder após Close")
	}
}

func TestAudioSequenceRestartResyncs(t *testing.T) {
	for _, tc := range []struct {
		name           string
		first, restart uint16
		late           int
	}{
		{"lower", 40000, 10000, peerResyncLate - 1},
		{"higher", 10000, 40000, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			codec := &orderedAudioCodec{}
			m, tx := audioTestManager(t, codec)
			defer m.cleanupMedia()
			var delivered []int
			m.OnPeerAudio = func(pcm []float32) { delivered = append(delivered, int(pcm[0])) }
			m.handleAudioRelayData(orderedAudioPacket(t, tx, tc.first, 160000))
			want := []int{int(tc.first)}
			for i := range 20 {
				seq := tc.restart + uint16(i)
				m.handleAudioRelayData(orderedAudioPacket(t, tx, seq, uint32(i*frame16)))
				if i >= tc.late {
					want = append(want, int(seq))
				}
			}
			if !slices.Equal(codec.order, want) || !slices.Equal(delivered, want) || m.PeerLateDrops() != int64(tc.late) {
				t.Fatalf("decode=%v PCM=%v late=%d quero=%v", codec.order, delivered, m.PeerLateDrops(), want)
			}
		})
	}
}

func TestAudioLateResyncCountsDistinctConsecutivePackets(t *testing.T) {
	var r peerAudioReorder
	now := time.Now()
	push := func(seq uint16) (bool, bool, bool) {
		return r.push(media.NewRtpHeader(core.PayloadTypeWhatsAppOpus, seq, 0, 20), []byte{1}, now)
	}
	push(40000)
	r.pop(now)
	for seq := uint16(10000); seq < 10007; seq++ {
		if dup, late, resync := push(seq); dup || !late || resync {
			t.Fatal("ressincronizou antes de oito atrasados")
		}
		if dup, _, resync := push(seq); !dup || resync {
			t.Fatal("duplicata contou para ressincronização")
		}
	}
	push(40001) // Áudio em ordem interrompe a sequência de atrasados.
	r.pop(now)
	push(40003) // Fica pendente e deve ser removido na ressincronização.
	for seq := uint16(10007); seq < 10015; seq++ {
		_, late, resync := push(seq)
		if last := seq == 10014; resync != last || late == last {
			t.Fatalf("seq=%d late=%v resync=%v", seq, late, resync)
		}
	}
	if r.next != 10014 || len(r.pending) != 1 || r.pending[0].header.SequenceNumber != 10014 {
		t.Fatal("ressincronização não limpou pendências")
	}
}

func TestAudioSSRCChangeResetsQueueAndTimeline(t *testing.T) {
	codec := &orderedAudioCodec{}
	m, tx := audioTestManager(t, codec)
	defer m.cleanupMedia()
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 40000, 0))
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 40002, 2*frame16))
	packet, err := tx.Protect(&media.RtpPacket{
		Header:  media.NewRtpHeader(core.PayloadTypeWhatsAppOpus, 10000, 0, 30),
		Payload: []byte{0x10, 0x27},
	})
	if err != nil {
		t.Fatal(err)
	}
	m.handleAudioRelayData(packet)
	if !slices.Equal(codec.order, []int{40000, 10000}) || len(m.audioReorder.pending) != 0 || m.audioBaseTs != 0 || m.audioPlayedSamples != frame16 {
		t.Fatalf("troca de SSRC reteve estado antigo: decode=%v", codec.order)
	}
	// Mesmo seq do fluxo anterior não é duplicata no novo SSRC.
	packet, err = tx.Protect(&media.RtpPacket{
		Header:  media.NewRtpHeader(core.PayloadTypeWhatsAppOpus, 40000, frame16, 30),
		Payload: []byte{0x40, 0x9c},
	})
	if err != nil {
		t.Fatal(err)
	}
	m.handleAudioRelayData(packet)
	if !slices.Equal(codec.order, []int{40000, 10000, 40000}) {
		t.Fatalf("histórico de duplicatas não foi limpo: %v", codec.order)
	}
}

func TestAudioUnauthenticatedPacketsDoNotAdvanceReorder(t *testing.T) {
	for _, first := range []bool{true, false} {
		t.Run(map[bool]string{true: "before first audio", false: "during audio"}[first], func(t *testing.T) {
			codec := &orderedAudioCodec{}
			m, tx := audioTestManager(t, codec)
			defer m.cleanupMedia()
			packets := make([][]byte, 3)
			for i := range packets {
				packets[i] = orderedAudioPacket(t, tx, uint16(i), uint32(i*frame16))
			}
			if !first {
				m.handleAudioRelayData(packets[0])
				m.handleAudioRelayData(packets[2])
			}
			forged := slices.Clone(packets[0])
			binary.BigEndian.PutUint16(forged[2:], 30000)
			m.handleAudioRelayData(forged)
			garbage := slices.Clone(forged[:12])
			m.handleAudioRelayData(garbage)
			binary.BigEndian.PutUint32(forged[8:], 30) // SSRC também precisa autenticação.
			m.handleAudioRelayData(forged)
			for _, p := range packets {
				m.handleAudioRelayData(p)
			}
			if !slices.Equal(codec.order, []int{0, 1, 2}) || m.audioReorder.next != 3 || m.audioReorder.ssrc != 20 || len(m.audioReorder.pending) != 0 {
				t.Fatalf("pacote inválido alterou áudio: decode=%v next=%d", codec.order, m.audioReorder.next)
			}
		})
	}
}

func TestAudioReorderTimerWithoutSRTPDoesNotDecode(t *testing.T) {
	codec := &orderedAudioCodec{}
	m, tx := audioTestManager(t, codec)
	defer m.cleanupMedia()
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 0, 0))
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 2, 2*frame16))
	m.mu.Lock()
	m.srtpSession = nil
	m.mu.Unlock()
	time.Sleep(2 * peerReorderWait)
	if codec.decodes.Load() != 1 {
		t.Fatal("timer usou mídia sem sessão SRTP")
	}
}

func TestAudioEmptyPayloadDoesNotTriggerPLC(t *testing.T) {
	codec := &orderedAudioCodec{}
	m, tx := audioTestManager(t, codec)
	defer m.cleanupMedia()
	m.handleAudioRelayData(orderedAudioPacket(t, tx, 0, 0))
	m.mu.Lock()
	now := time.Now()
	m.audioReorder.push(media.NewRtpHeader(core.PayloadTypeWhatsAppOpus, 4, 4*frame16, 20), nil, now)
	pcm := m.drainPeerAudioLocked(now.Add(peerReorderWait))
	m.mu.Unlock()
	if len(pcm) != 0 || codec.decodes.Load() != 1 || m.audioPlayedSamples != frame16 {
		t.Fatal("payload vazio acionou decoder/PLC ou alterou timeline")
	}
}
