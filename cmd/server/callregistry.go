package main

import (
	"sync"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/media"
)

type activeCall struct {
	cm          *call.CallManager
	bridge      *Bridge
	wsBridge    *wsBridge // ponte WebSocket (alternativa ao pion WebRTC para proxies HTTP)
	browserOpus media.Codec
	recorder    *callRecorder // nil quando a gravação está desligada na sessão

	// [pacing BR 12/08] Jitter buffer + playout PACEADO do leg WhatsApp->navegador.
	// OnPeerAudio empilha PCM 48k aqui; o loop startBrowserPlayout drena a 20ms
	// REGULARES e escreve no browser. Sem isto, o motor repassava o áudio na cadência
	// de chegada da Meta (com o jitter dela), e o NetEQ do Chrome inflava o buffer p/
	// ~1s de atraso. Guardado por playoutMu; parado (stopPlayout/playoutDone) ANTES de
	// fechar browserOpus (o Encode é cgo — não pode rodar sobre o codec já liberado).
	playoutMu   sync.Mutex
	playout     []float32
	primed      bool
	stopPlayout chan struct{}
	playoutDone chan struct{}
	// [drift 12/08] instrumentação (sob playoutMu): amostras 48k empilhadas (entrada) e
	// descartadas no teto. O loop loga a TAXA de saída vs entrada a cada 5s p/ achar o drift.
	inSamples int64
	dropped   int64
	// [P1 12/08] Serializa o USO do codec/ponte no loop de playout (Encode/WriteOpus) com a
	// TROCA+Close de bridge/browserOpus em setBridge. Sem isto, oldOC.Close()
	// (opus_encoder_destroy) podia rodar durante o oc.Encode do loop → use-after-free no cgo
	// → segfault do motor (derruba TODAS as chamadas da célula). Atingível a cada re-offer do
	// navegador (reload/ICE-restart/transferência).
	mediaMu sync.Mutex
}

type callRegistry struct {
	mu    sync.Mutex
	calls map[string]*activeCall
}

func newCallRegistry() *callRegistry {
	return &callRegistry{calls: map[string]*activeCall{}}
}

func (r *callRegistry) add(callID string, ac *activeCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[callID] = ac
}

func (r *callRegistry) get(callID string) (*activeCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	return ac, ok
}

// [pacing BR 12/08] Snapshot de bridge+codec sob o lock (bridge/browserOpus são escritos
// por setBridge sob r.mu). O loop de playout lê por aqui a cada tick — race-free, e enxerga
// o swap de ponte (renegociação/transferência) sem gap de áudio.
func (r *callRegistry) mediaOf(callID string) (*Bridge, media.Codec, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, nil, false
	}
	return ac.bridge, ac.browserOpus, true
}

func (r *callRegistry) remove(callID string) (*activeCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, false
	}
	delete(r.calls, callID)
	return ac, true
}

func (r *callRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *callRegistry) setBridge(callID string, b *Bridge, oc media.Codec) (*Bridge, media.Codec, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, nil, false
	}
	oldB, oldOC := ac.bridge, ac.browserOpus
	ac.bridge, ac.browserOpus = b, oc
	return oldB, oldOC, true
}

func (r *callRegistry) setWSBridge(callID string, b *wsBridge, oc media.Codec) (*wsBridge, *Bridge, media.Codec, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, nil, nil, false
	}
	oldWSB, oldB, oldOC := ac.wsBridge, ac.bridge, ac.browserOpus
	ac.wsBridge, ac.bridge, ac.browserOpus = b, nil, oc
	return oldWSB, oldB, oldOC, true
}

func (r *callRegistry) drain() []*activeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*activeCall, 0, len(r.calls))
	for _, ac := range r.calls {
		out = append(out, ac)
	}
	r.calls = map[string]*activeCall{}
	return out
}
