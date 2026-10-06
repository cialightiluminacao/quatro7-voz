package main

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"
)

// Testes do buffer de playout adaptativo (leg WhatsApp->navegador). Tudo aqui é Go puro:
// o relógio é o próprio tick de 20ms (cada next() = 20ms), então as simulações são
// determinísticas e não dependem de time.Sleep nem do codec cgo.

const smpPerMs = 48 // PCM 48kHz

func blocoConst(n int, v float32) []float32 {
	b := make([]float32, n)
	for i := range b {
		b[i] = v
	}
	return b
}

func TestPlayoutBuffer_PrimeInicialNoAlvo(t *testing.T) {
	var p playoutBuffer
	if got := p.targetMs(); got != 60 {
		t.Fatalf("alvo inicial = %dms, quero 60ms", got)
	}
	p.push(blocoConst(40*smpPerMs, 0.5))
	if _, emit := p.next(); emit {
		t.Fatal("com 40ms (< alvo 60ms) não pode emitir no arranque")
	}
	p.push(blocoConst(20*smpPerMs, 0.5))
	f, emit := p.next()
	if !emit || f == nil || len(f) != playoutFrame48 {
		t.Fatalf("com 60ms deveria emitir um frame real de 20ms: emit=%v len=%d", emit, len(f))
	}
}

func TestPlayoutBuffer_UnderrunEmiteSilencioSobeAlvoERePrima(t *testing.T) {
	var p playoutBuffer
	p.push(blocoConst(60*smpPerMs, 0.5))
	for i := 0; i < 3; i++ {
		if f, emit := p.next(); !emit || f == nil {
			t.Fatalf("tick %d: esperava áudio real", i)
		}
	}
	// buffer vazio: underrun → silêncio (emit=true, frame nil), cadência mantida
	f, emit := p.next()
	if !emit || f != nil {
		t.Fatalf("underrun deve emitir SILÊNCIO mantendo a cadência: emit=%v frame=%v", emit, f != nil)
	}
	if p.targetMs() != 80 || p.underruns != 1 {
		t.Fatalf("underrun deve subir o alvo p/ 80ms e contar 1: alvo=%d underruns=%d", p.targetMs(), p.underruns)
	}
	// Ticks seguidos sem áudio = MESMO episódio (não sobe o alvo a cada tick).
	p.next()
	if p.targetMs() != 80 || p.underruns != 1 {
		t.Fatalf("episódio contínuo não pode contar 2×: alvo=%d underruns=%d", p.targetMs(), p.underruns)
	}
	// Re-prime: com 40ms (< alvo 80) continua silêncio, mas EMITINDO (sem buraco de RTP).
	p.push(blocoConst(40*smpPerMs, 0.5))
	if f, emit := p.next(); !emit || f != nil {
		t.Fatalf("re-prime abaixo do alvo: quero silêncio emitido, emit=%v real=%v", emit, f != nil)
	}
	p.push(blocoConst(40*smpPerMs, 0.5))
	if f, emit := p.next(); !emit || f == nil {
		t.Fatal("com 80ms acumulados deveria voltar o áudio real")
	}
	if p.underruns != 1 {
		t.Fatalf("underruns = %d, quero 1", p.underruns)
	}
}

func TestPlayoutBuffer_AlvoTemTetoEDesceDevagar(t *testing.T) {
	var p playoutBuffer
	// 30 underruns isolados → alvo travado em 400ms.
	for i := 0; i < 30; i++ {
		p.push(blocoConst(p.target+playoutFrame48, 0.5))
		for {
			f, _ := p.next()
			if f == nil {
				break
			}
		}
	}
	if p.targetMs() != 400 {
		t.Fatalf("alvo após muitos underruns = %dms, quero teto 400ms", p.targetMs())
	}
	// Estável (sempre com áudio): -10ms a cada 10s (500 ticks).
	p.push(blocoConst(500*smpPerMs, 0.1))
	for i := 0; i < 499; i++ {
		p.push(blocoConst(playoutFrame48, 0.1))
		p.next()
	}
	if p.targetMs() != 400 {
		t.Fatalf("antes de 10s estáveis o alvo não desce: %d", p.targetMs())
	}
	p.push(blocoConst(playoutFrame48, 0.1))
	p.next()
	if p.targetMs() != 390 {
		t.Fatalf("após 10s estáveis alvo = %dms, quero 390ms", p.targetMs())
	}
	// Piso de 60ms.
	for i := 0; i < 500*40; i++ {
		p.push(blocoConst(playoutFrame48, 0.1))
		p.next()
	}
	if p.targetMs() != 60 {
		t.Fatalf("piso do alvo = %dms, quero 60ms", p.targetMs())
	}
}

func TestPlayoutBuffer_TetoDuroCortaAteOAlvo(t *testing.T) {
	var p playoutBuffer
	p.push(blocoConst(60*smpPerMs, 0.5))
	p.next()
	p.push(blocoConst(1200*smpPerMs, 0.5)) // rajada absurda (> teto duro de 1000ms)
	if p.hardDropped != 0 {
		t.Fatal("o corte duro é no tick (vê a rajada inteira), não no push")
	}
	if f, _ := p.next(); f == nil {
		t.Fatal("após o corte duro o tick toca áudio real")
	}
	if lv := p.level(); lv != p.target-playoutFrame48 {
		t.Fatalf("acima do teto duro deve cortar até o alvo: nível=%dms (quero alvo %dms - 20ms do tick)", lv/smpPerMs, p.targetMs())
	}
	if p.hardDropped == 0 || p.trimmed != 0 {
		t.Fatalf("contadores: hard=%d trimmed=%d", p.hardDropped, p.trimmed)
	}
	// Até o teto duro NÃO corta na chegada (o teto antigo de 200ms cortava).
	var q playoutBuffer
	q.push(blocoConst(900*smpPerMs, 0.5))
	if q.level() != 900*smpPerMs || q.hardDropped != 0 {
		t.Fatalf("900ms não pode ser cortado na chegada: nível=%d hard=%d", q.level(), q.hardDropped)
	}
}

func TestPlayoutBuffer_DescarteSuaveUmBlocoPor250msPreferindoSilencio(t *testing.T) {
	var p playoutBuffer
	// 800ms de fala alta, com UM bloco de 20ms quase mudo perto do fim (pausa da voz).
	alto := blocoConst(800*smpPerMs, 0.5)
	for i := 38 * playoutFrame48; i < 39*playoutFrame48; i++ {
		alto[i] = 0.001
	}
	p.push(alto)
	var trims []int
	for tick := 0; tick < 100; tick++ { // 2s
		p.push(blocoConst(playoutFrame48, 0.5)) // entrada = saída: excesso sustentado
		before := p.trimmed
		p.next()
		if p.trimmed > before {
			if p.trimmed-before != int64(playoutFrame48) {
				t.Fatalf("descarte suave deve ser de UM bloco de 20ms, foi %d amostras", p.trimmed-before)
			}
			trims = append(trims, tick)
		}
	}
	if len(trims) == 0 {
		t.Fatal("nível ~780ms (> alvo+100) por 2s: deveria ter descarte suave")
	}
	for i := 1; i < len(trims); i++ {
		if (trims[i]-trims[i-1])*20 < 250 {
			t.Fatalf("descartes a menos de 250ms: ticks %v", trims)
		}
	}
	if p.hardDropped != 0 {
		t.Fatalf("abaixo do teto duro não pode haver corte duro: %d", p.hardDropped)
	}
	// O 1º descarte tem que ter escolhido o bloco mudo: das 960 amostras ~0.001 sobra no
	// máximo a rampa do crossfade.
	quietas := 0
	for _, v := range p.buf {
		if v > 0 && v < 0.01 {
			quietas++
		}
	}
	if quietas > playoutTrimFade48 {
		t.Fatalf("%d amostras do bloco quase mudo ainda no buffer: o descarte não preferiu o silêncio", quietas)
	}
}

// ---------- Simulação de chegada em rajadas ----------

// legacyPlayout reproduz o comportamento ANTERIOR (frota6b): prime 40ms, teto FIXO de
// 200ms descartando o excedente na chegada, silêncio no underrun sem re-prime.
type legacyPlayout struct {
	buf     []float32
	primed  bool
	dropped int64
	onDrop  func([]float32)
}

func (l *legacyPlayout) push(pcm []float32) {
	l.buf = append(l.buf, pcm...)
	if max := playoutFrame48 * 10; len(l.buf) > max {
		cut := len(l.buf) - max
		if l.onDrop != nil {
			l.onDrop(l.buf[:cut])
		}
		l.dropped += int64(cut)
		l.buf = l.buf[cut:]
	}
}

func (l *legacyPlayout) next() ([]float32, bool) {
	if !l.primed {
		if len(l.buf) < playoutFrame48*2 {
			return nil, false
		}
		l.primed = true
	}
	if len(l.buf) < playoutFrame48 {
		return nil, true
	}
	f := l.buf[:playoutFrame48:playoutFrame48]
	l.buf = l.buf[playoutFrame48:]
	return f, true
}

func (l *legacyPlayout) level() int { return len(l.buf) }

type playoutModel interface {
	push([]float32)
	next() ([]float32, bool)
	level() int
}

type simFrame struct {
	atMs int
	pcm  []float32
}

const vozLimiar = 0.05 // |amostra| acima disto = voz (fala sintética tem amplitude 0.3)

// sinalVoz: fala sintética (300Hz, 0.3) 1,2s / pausa (ruído baixo, 0.002) 0,6s, contínua.
func sinalVoz(startSmp, n int) []float32 {
	out := make([]float32, n)
	for i := range out {
		s := startSmp + i
		if (s/smpPerMs)%1800 < 1200 {
			out[i] = float32(0.3 * math.Sin(2*math.Pi*300*float64(s)/48000))
		} else {
			out[i] = float32(0.002 * math.Sin(2*math.Pi*50*float64(s)/48000))
		}
	}
	return out
}

// framesComJitter: frames de 60ms (MLow) com atraso de rede D em [0, 2*jitter] ms, ou
// seja, ±jitter em volta de um atraso médio. Dois modelos:
//   - rajada=false: D independente por frame (uniforme) — reordena o que cruzar.
//   - rajada=true: D é um passeio aleatório (passo U(-40,+40)ms por frame, refletido nas
//     bordas) e a ordem é preservada — o atraso SOBE e DESCE aos poucos, então o áudio
//     chega em RAJADAS (vários frames juntos depois de um vazio), como o relay da Meta.
//
// stall=[ini,fim) em ms: todo frame com chegada nesse intervalo só chega em fim (pausa
// da rede seguida de rajada com tudo o que atrasou).
func framesComJitter(seed uint64, durMs, jitterMs int, rajada bool, stallIni, stallFim int) []simFrame {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	var fs []simFrame
	d, prev := jitterMs, 0
	for nom := 0; nom < durMs; nom += 60 {
		if rajada {
			d += rng.IntN(81) - 40
			if d < 0 {
				d = -d
			}
			if d > 2*jitterMs {
				d = 4*jitterMs - d
			}
		} else {
			d = rng.IntN(2*jitterMs + 1)
		}
		at := nom + d
		if rajada && at < prev {
			at = prev
		}
		if at >= stallIni && at < stallFim {
			at = stallFim
		}
		prev = at
		fs = append(fs, simFrame{atMs: at, pcm: sinalVoz(nom*smpPerMs, 60*smpPerMs)})
	}
	sort.SliceStable(fs, func(i, j int) bool { return fs[i].atMs < fs[j].atMs })
	return fs
}

// framesTaxaOscilante: imita o log real de 06/10 (janelas de 5s com in_x_realtime
// alternando 0,61 e 1,39): o mesmo áudio chega mais devagar e depois recupera.
func framesTaxaOscilante(durMs int) []simFrame {
	var fs []simFrame
	at := 0.0
	for nom := 0; nom < durMs; nom += 60 {
		fs = append(fs, simFrame{atMs: int(at), pcm: sinalVoz(nom*smpPerMs, 60*smpPerMs)})
		rate := 0.61
		if (int(at)/5000)%2 == 1 {
			rate = 1.39
		}
		at += 60 / rate
	}
	return fs
}

type simResult struct {
	inSmp, dropSmp     int64
	inVoz, dropVoz     int64
	meanLevelMs        float64
	underrunEps        int // episódios de silêncio após o aquecimento
	silentTicks        int // ticks de silêncio após o aquecimento
	maxSilentRun       int // maior sequência de ticks de silêncio após o aquecimento
	levelAfterMs       []int
	finalTargetMs      int
	trimmed, hardDrops int64
}

func (r simResult) lossPct() float64 { return 100 * float64(r.dropSmp) / float64(r.inSmp) }
func (r simResult) vozPct() float64  { return 100 * float64(r.dropVoz) / float64(r.inVoz) }

func contaVoz(pcm []float32) (n int64) {
	for _, v := range pcm {
		if v > vozLimiar || v < -vozLimiar {
			n++
		}
	}
	return n
}

// simular roda o modelo em ms: entrega os frames que chegaram e, a cada 20ms, um tick.
// Métricas de underrun/latência contam só depois de warmupMs (fase de adaptação).
func simular(m playoutModel, fs []simFrame, durMs, warmupMs int, setDrop func(func([]float32))) simResult {
	var r simResult
	setDrop(func(d []float32) {
		r.dropSmp += int64(len(d))
		r.dropVoz += contaVoz(d)
	})
	idx := 0
	var lvlSum float64
	var lvlN, run int
	inSilence := false
	for ms := 0; ms <= durMs; ms++ {
		for idx < len(fs) && fs[idx].atMs <= ms {
			r.inSmp += int64(len(fs[idx].pcm))
			r.inVoz += contaVoz(fs[idx].pcm)
			m.push(fs[idx].pcm)
			idx++
		}
		if ms%20 != 0 {
			continue
		}
		f, emit := m.next()
		if !emit {
			continue
		}
		lv := m.level() / smpPerMs
		r.levelAfterMs = append(r.levelAfterMs, lv)
		if ms < warmupMs {
			continue
		}
		lvlSum += float64(lv)
		lvlN++
		if f == nil {
			r.silentTicks++
			run++
			if run > r.maxSilentRun {
				r.maxSilentRun = run
			}
			if !inSilence {
				r.underrunEps++
			}
			inSilence = true
		} else {
			run = 0
			inSilence = false
		}
	}
	if lvlN > 0 {
		r.meanLevelMs = lvlSum / float64(lvlN)
	}
	return r
}

func rodaNovo(fs []simFrame, durMs, warmupMs int) simResult {
	p := &playoutBuffer{}
	r := simular(p, fs, durMs, warmupMs, func(f func([]float32)) { p.onDrop = f })
	r.finalTargetMs, r.trimmed, r.hardDrops = p.targetMs(), p.trimmed, p.hardDropped
	return r
}

func rodaAntigo(fs []simFrame, durMs, warmupMs int) simResult {
	l := &legacyPlayout{}
	return simular(l, fs, durMs, warmupMs, func(f func([]float32)) { l.onDrop = f })
}

func logResult(t *testing.T, nome string, r simResult) {
	t.Helper()
	t.Logf("%-7s perda=%5.2f%% (voz %5.2f%%) latência_média=%4.0fms underruns=%d silêncio=%d ticks (maior %d) alvo_final=%dms suave=%d duro=%d",
		nome, r.lossPct(), r.vozPct(), r.meanLevelMs, r.underrunEps, r.silentTicks, r.maxSilentRun,
		r.finalTargetMs, r.trimmed/int64(playoutFrame48), r.hardDrops)
}

// Regime normal: frames de 60ms com jitter ±150ms, 3 min, 5 sementes, nos dois modelos de
// chegada. Achado: com jitter de ±150ms o antigo perde só ~1-4% — a perda ALTA do log real
// (~20-27%) só aparece com oscilações de SEGUNDOS (ver TaxaOscilanteDoLogReal).
func TestPlayoutSimulacao_RajadasJitter150(t *testing.T) {
	const dur, warm = 180_000, 30_000
	const ticksPosAquecimento = (dur - warm) / 20
	for _, rajada := range []bool{true, false} {
		for seed := uint64(1); seed <= 5; seed++ {
			fs := framesComJitter(seed, dur, 150, rajada, -1, -1)
			antigo := rodaAntigo(fs, dur, warm)
			novo := rodaNovo(fs, dur, warm)
			t.Logf("rajada=%v seed=%d", rajada, seed)
			logResult(t, "antigo", antigo)
			logResult(t, "novo", novo)
			if novo.lossPct() >= 3 || novo.lossPct() >= antigo.lossPct() {
				t.Errorf("rajada=%v seed %d: perda nova %.2f%% (quero < 3%% e < antiga %.2f%%)", rajada, seed, novo.lossPct(), antigo.lossPct())
			}
			if novo.meanLevelMs >= 300 {
				t.Errorf("rajada=%v seed %d: latência média %.0fms (quero < 300ms)", rajada, seed, novo.meanLevelMs)
			}
			// Após a adaptação: underrun raro (< 1%% do tempo, no máx. 1 a cada 15s) e nunca
			// contínuo (nenhum buraco > 200ms).
			if novo.silentTicks*100 >= ticksPosAquecimento || novo.underrunEps > 10 || novo.maxSilentRun > 10 {
				t.Errorf("rajada=%v seed %d: underrun após adaptação: episódios=%d silêncio=%d ticks maior=%d",
					rajada, seed, novo.underrunEps, novo.silentTicks, novo.maxSilentRun)
			}
			if novo.underrunEps*5 > antigo.underrunEps {
				t.Errorf("rajada=%v seed %d: picota quase como o antigo: %d × %d episódios", rajada, seed, novo.underrunEps, antigo.underrunEps)
			}
			if novo.hardDrops != 0 {
				t.Errorf("rajada=%v seed %d: corte duro no regime normal: %d", rajada, seed, novo.hardDrops)
			}
		}
	}
}

// Pausa de 1,5s da rede seguida de rajada (todo o atrasado chega de uma vez).
func TestPlayoutSimulacao_PausaDe1500msSeguidaDeRajada(t *testing.T) {
	const dur, warm = 120_000, 30_000
	const stallIni, stallFim = 60_000, 61_500
	fs := framesComJitter(7, dur, 150, true, stallIni, stallFim)
	antigo := rodaAntigo(fs, dur, warm)
	novo := rodaNovo(fs, dur, warm)
	logResult(t, "antigo", antigo)
	logResult(t, "novo", novo)
	if novo.lossPct() >= antigo.lossPct() {
		t.Errorf("novo (%.2f%%) não pode perder mais que o antigo (%.2f%%)", novo.lossPct(), antigo.lossPct())
	}
	// O buffer nunca passa do teto duro e volta ao regime (< 300ms) em até 3s após a rajada.
	maxLv := 0
	for i, lv := range novo.levelAfterMs {
		if lv > maxLv {
			maxLv = lv
		}
		ms := i * 20
		if ms > stallFim+3000 && ms < stallFim+20_000 && lv >= 400 {
			t.Fatalf("nível %dms em t=%dms: não voltou ao regime após a rajada", lv, ms)
		}
	}
	if maxLv > 1000 {
		t.Errorf("nível máximo %dms passou do teto duro", maxLv)
	}
	if novo.meanLevelMs >= 300 {
		t.Errorf("latência média %.0fms (quero < 300ms)", novo.meanLevelMs)
	}
}

// Padrão do log real (taxa oscilando 0,61×/1,39× em janelas de 5s): é ESTE padrão que
// reproduz a perda alta do teto fixo de 200ms. Aqui uma parte da perda é inevitável
// (≈2s de atraso acumulado por ciclo: ou toca 2s atrasado ou descarta), mas o novo tem
// de perder menos e trocar o picotado (centenas de micro-buracos) por poucos eventos.
func TestPlayoutSimulacao_TaxaOscilanteDoLogReal(t *testing.T) {
	const dur, warm = 120_000, 10_000
	fs := framesTaxaOscilante(dur)
	antigo := rodaAntigo(fs, dur, warm)
	novo := rodaNovo(fs, dur, warm)
	logResult(t, "antigo", antigo)
	logResult(t, "novo", novo)
	if antigo.lossPct() < 15 {
		t.Errorf("o antigo deveria reproduzir a perda ALTA do log real, deu %.2f%%", antigo.lossPct())
	}
	if novo.lossPct() >= antigo.lossPct() {
		t.Errorf("novo (%.2f%%) não pode perder mais que o antigo (%.2f%%)", novo.lossPct(), antigo.lossPct())
	}
	if novo.underrunEps*5 > antigo.underrunEps {
		t.Errorf("novo ainda picota: %d episódios de underrun × %d do antigo", novo.underrunEps, antigo.underrunEps)
	}
}
