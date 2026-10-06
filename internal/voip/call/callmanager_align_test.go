package call

import "testing"

// [frota6c 06/10] O relay entrega o RTP por DataChannel SCTP NÃO ordenado (e com até 2
// relays abertos): um pacote atrasado (retransmitido) ou duplicado chega DEPOIS de
// pacotes mais novos. Antes, alignPeerAudio tratava isso como "salto para trás",
// reiniciava a timeline e devolvia o áudio velho — que ia para o fim do buffer (fora de
// ordem) e ainda fazia o pacote seguinte ganhar um padding de silêncio falso.

const frame16 = 960 // 60ms @ 16kHz (MLow)

func pcmFrame() []float32 {
	p := make([]float32, frame16)
	for i := range p {
		p[i] = 0.5
	}
	return p
}

func TestAlignPeerAudio_AtrasadoOuDuplicadoNaoInflaATimeline(t *testing.T) {
	m := &CallManager{}
	total := 0
	feed := func(ts uint32) int {
		n := len(m.alignPeerAudio(ts, pcmFrame()))
		total += n
		return n
	}
	feed(0)
	feed(1 * frame16)
	// pacote 2 perdido por ora: o 3 chega e a lacuna vira silêncio (como antes)
	if n := feed(3 * frame16); n != 2*frame16 {
		t.Fatalf("lacuna de 1 pacote: quero padding de 60ms + 60ms, veio %d", n)
	}
	// o 2 chega ATRASADO (retransmissão): o slot já foi tocado como silêncio → descarta
	if n := feed(2 * frame16); n != 0 {
		t.Fatalf("pacote atrasado deve ser descartado, veio %d amostras", n)
	}
	if n := feed(4 * frame16); n != frame16 {
		t.Fatalf("pacote seguinte ao atrasado ganhou padding falso: %d amostras (quero %d)", n, frame16)
	}
	// duplicata do último (2º relay)
	if n := feed(4 * frame16); n != 0 {
		t.Fatalf("duplicata deve ser descartada, veio %d amostras", n)
	}
	if total != 5*frame16 {
		t.Fatalf("timeline: %d amostras para 5 slots de 60ms (quero %d)", total, 5*frame16)
	}
	if got := m.PeerLateDrops(); got != 2 {
		t.Fatalf("PeerLateDrops = %d, quero 2", got)
	}
}

func TestAlignPeerAudio_SaltoGrandeContinuaReiniciando(t *testing.T) {
	m := &CallManager{}
	m.alignPeerAudio(100_000, pcmFrame())
	// salto para trás maior que a janela (stream reiniciado): reinicia e entrega
	if n := len(m.alignPeerAudio(10, pcmFrame())); n != frame16 {
		t.Fatalf("salto grande para trás deve reiniciar a timeline, veio %d", n)
	}
	if n := len(m.alignPeerAudio(10+frame16, pcmFrame())); n != frame16 {
		t.Fatalf("após reiniciar, o seguinte deve vir sem padding, veio %d", n)
	}
	// salto grande para frente: também reinicia (sem padding gigante)
	if n := len(m.alignPeerAudio(1_000_000, pcmFrame())); n != frame16 {
		t.Fatalf("salto grande para frente deve reiniciar, veio %d", n)
	}
	if m.PeerLateDrops() != 0 {
		t.Fatalf("reinício não é descarte de atrasado: %d", m.PeerLateDrops())
	}
}
