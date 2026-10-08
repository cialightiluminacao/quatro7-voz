package main

// Buffer de playout adaptativo WhatsApp->navegador. Cada next() = 20ms.
// Alvo 60–400ms, silêncio no underrun/re-prime e decaimento após 10s estáveis.
// Acima de alvo+60ms, comprime o tempo retirando no máximo 1 frame a cada 5 ticks,
// com crossfade. O teto de 2000ms retira somente o excedente, mais antigo primeiro.
// Não é thread-safe: o chamador segura activeCall.playoutMu.

const (
	playoutFrame48        = 960      // 20ms @ 48kHz
	playoutMinTarget48    = 60 * 48  // alvo inicial e piso
	playoutMaxTarget48    = 400 * 48 // teto do alvo
	playoutTargetUp48     = 20 * 48  // por episódio de underrun
	playoutTargetDown48   = 10 * 48  // por janela estável
	playoutDecayTicks     = 500      // 10s sem underrun
	playoutHardMax48      = 2000 * 48
	playoutTrimMargin48   = 60 * 48 // excesso tolerado acima do alvo
	playoutTrimEveryTicks = 5       // >=100ms entre descartes suaves
	playoutTrimFade48     = 96      // crossfade de 2ms na emenda do descarte
)

type playoutBuffer struct {
	buf    []float32
	target int // amostras; 0 = ainda não inicializado (zero value usável)
	primed bool
	// rePriming: em underrun desde o tick anterior; segura o áudio até o nível voltar ao
	// alvo (emitindo silêncio) e conta o episódio uma vez só.
	rePriming      bool
	ticksStable    int // ticks com áudio real desde o último underrun/decaimento
	ticksSinceTrim int

	// Métricas acumuladas (o loop loga os deltas no [voz-drift]).
	in          int64 // amostras de entrada
	underruns   int64 // episódios
	trimmed     int64 // amostras do descarte suave
	hardDropped int64 // amostras do teto duro

	onDrop func([]float32) // só testes: observa o que foi descartado
}

func (p *playoutBuffer) init() {
	if p.target == 0 {
		p.target = playoutMinTarget48
	}
}

func (p *playoutBuffer) level() int    { return len(p.buf) }
func (p *playoutBuffer) targetMs() int { p.init(); return p.target / 48 }

func (p *playoutBuffer) dropped() int64 { return p.trimmed + p.hardDropped }

// push enfileira PCM 48k vindo do WhatsApp (OnPeerAudio).
func (p *playoutBuffer) push(pcm []float32) {
	p.init()
	p.in += int64(len(pcm))
	p.buf = append(p.buf, pcm...)
}

// capExcess roda no tick, quando a rajada inteira já chegou.
func (p *playoutBuffer) capExcess() {
	if len(p.buf) <= playoutHardMax48 {
		return
	}
	// Arredonda para frames de 20ms, preservando o alinhamento do PCM restante.
	excess := len(p.buf) - playoutHardMax48
	cut := (excess + playoutFrame48 - 1) / playoutFrame48 * playoutFrame48
	if p.onDrop != nil {
		p.onDrop(p.buf[:cut])
	}
	p.hardDropped += int64(cut)
	p.buf = append(p.buf[:0], p.buf[cut:]...)
}

// flush descarta áudio velho na retomada, mantendo alvo e métricas cumulativas.
// Se já houve prime, o próximo tick segue emitindo silêncio até re-prime.
func (p *playoutBuffer) flush() {
	p.buf = nil
	p.rePriming = p.primed
	p.ticksStable, p.ticksSinceTrim = 0, 0
}

// next devolve o frame de 20ms deste tick. emit=false só antes do 1º prime (nada a
// enviar ainda). frame==nil com emit=true = SILÊNCIO (underrun/re-prime): o chamador
// envia silêncio para manter a cadência. O frame devolvido é uma cópia.
func (p *playoutBuffer) next() (frame []float32, emit bool) {
	p.init()
	if !p.primed {
		if len(p.buf) < p.target {
			return nil, false
		}
		p.primed = true
	}
	p.ticksSinceTrim++
	p.capExcess()
	if p.rePriming && len(p.buf) >= p.target {
		p.rePriming = false
	}
	if p.rePriming || len(p.buf) < playoutFrame48 {
		if !p.rePriming {
			p.rePriming = true
			p.underruns++
			p.target = min(p.target+playoutTargetUp48, playoutMaxTarget48)
		}
		p.ticksStable = 0
		return nil, true
	}
	frame = make([]float32, playoutFrame48)
	copy(frame, p.buf)
	p.buf = p.buf[playoutFrame48:]

	p.ticksStable++
	if p.ticksStable >= playoutDecayTicks {
		p.ticksStable = 0
		p.target = max(p.target-playoutTargetDown48, playoutMinTarget48)
	}
	if len(p.buf) > p.target+playoutTrimMargin48 && p.ticksSinceTrim >= playoutTrimEveryTicks {
		p.trimQuietest()
		p.ticksSinceTrim = 0
	}
	return frame, true
}

// trimQuietest remove o bloco de 20ms de MENOR energia (exceto o primeiro, que é o
// próximo a tocar) e emenda as pontas com um crossfade curto.
func (p *playoutBuffer) trimQuietest() {
	n := len(p.buf) / playoutFrame48
	if n < 2 {
		return
	}
	best, bestE := -1, 0.0
	for k := 1; k < n; k++ {
		e := 0.0
		for _, v := range p.buf[k*playoutFrame48 : (k+1)*playoutFrame48] {
			e += float64(v) * float64(v)
		}
		if best < 0 || e < bestE {
			best, bestE = k, e
		}
	}
	s, d := best*playoutFrame48, playoutFrame48
	if p.onDrop != nil {
		p.onDrop(p.buf[s : s+d])
	}
	// Overlap: os últimos F antes do corte viram a rampa (pré -> pós), onde "pós" são as
	// F amostras que antecedem s+d — assim a saída emenda contínua em buf[s+d:].
	const f = playoutTrimFade48
	for i := 0; i < f; i++ {
		w := float32(i+1) / float32(f+1)
		p.buf[s-f+i] = p.buf[s-f+i]*(1-w) + p.buf[s+d-f+i]*w
	}
	p.buf = append(p.buf[:s], p.buf[s+d:]...)
	p.trimmed += int64(d)
}
