package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/signaling"
	"wacalls/internal/voip/wanode"
	"wacalls/internal/wa"

	"database/sql"

	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type Session struct {
	id   string
	name string
	mgr  *SessionManager
	log  *slog.Logger

	client *whatsmeow.Client
	reg    *callRegistry

	// store próprio desta sessão (1 banco por sessão)
	waContainer *sqlstore.Container
	waDB        *sql.DB

	mu            sync.Mutex
	auth          AuthSnapshot
	webhook       string
	webhookSecret string   // assina o payload (X-Webhook-Signature); vazio = sem assinatura
	webhookEvents []string // só entrega estes tipos de evento; vazio = todos
	chatwoot      ChatwootConfig
	recording     bool   // grava as chamadas desta sessão (opt-in)
	proxy         string // proxy de saída da conexão WhatsApp (http/https/socks5)

	// downAlerted evita repetir o aviso de "sessão desconectada" no Chatwoot
	// enquanto ela segue caída; volta a false ao reconectar (events.Connected).
	downAlerted bool

	// linked diz se o CELULAR concluiu o vínculo deste aparelho. O pareamento pelo
	// QR fecha do lado do servidor (pair-success) antes do celular terminar: ele
	// ainda precisa entregar a chave de app state. Se o celular falha nessa etapa
	// (o "carrega e dá erro" no WhatsApp), o motor ficava "open" em cima de um
	// vínculo pela metade e o painel mostrava conectado. Só vira true depois que
	// a chave chega; volta a false ao trocar o client (novo pareamento).
	linked bool

	// sentIDs guarda os IDs de mensagens que ESTE cliente enviou, com a origem
	// (API ou agente do Chatwoot), para decidir o que fazer quando voltarem como
	// evento from_me. msgID -> selfSent.
	sentIDs sync.Map

	// importedMsgIDs deduplica a importação de histórico entre os chunks do
	// HistorySync (e reconexões enquanto o processo vive). msgID -> struct{}.
	importedMsgIDs sync.Map
	// importMu serializa a importação de histórico: os chunks do HistorySync
	// chegam em goroutines separadas e criariam contatos/conversas duplicados no
	// Chatwoot se rodassem ensureContact/ensureConversation concorrentemente.
	importMu sync.Mutex
}

// Origem de uma mensagem enviada por nós. O agente do Chatwoot nunca é espelhado
// (a mensagem já está na conversa); a API é espelhada quando o toggle mirror_api
// da sessão está ligado.
const (
	selfSentAPI      = "api"
	selfSentChatwoot = "chatwoot"
)

type selfSent struct {
	origin string
	ts     int64
}

// markSelfSent registra uma mensagem enviada por nós (com prune do que é antigo).
func (s *Session) markSelfSent(id, origin string) {
	if id == "" {
		return
	}
	now := time.Now().UnixMilli()
	s.sentIDs.Store(id, selfSent{origin: origin, ts: now})
	s.sentIDs.Range(func(k, v any) bool {
		if e, ok := v.(selfSent); ok && now-e.ts > 10*60*1000 {
			s.sentIDs.Delete(k)
		}
		return true
	})
}

// selfSentOrigin diz se a mensagem foi enviada por nós e por qual caminho
// (selfSentAPI/selfSentChatwoot). ok=false => veio do aparelho.
func (s *Session) selfSentOrigin(id string) (origin string, ok bool) {
	v, found := s.sentIDs.Load(id)
	if !found {
		return "", false
	}
	e, cast := v.(selfSent)
	if !cast {
		return "", false
	}
	return e.origin, true
}

// sendAndMark envia uma mensagem, a registra como "enviada por nós" e devolve o
// ID da mensagem do WhatsApp (usado p/ gravar o source_id no Chatwoot).
func (s *Session) sendAndMark(ctx context.Context, jid types.JID, msg *waE2E.Message) (string, error) {
	resp, err := s.client.SendMessage(ctx, jid, msg)
	if err != nil {
		return "", err
	}
	s.markSelfSent(resp.ID, selfSentChatwoot)
	return resp.ID, nil
}

func (s *Session) setWebhook(url, secret string, events []string) {
	s.mu.Lock()
	s.webhook = url
	s.webhookSecret = secret
	s.webhookEvents = events
	s.mu.Unlock()
}

func (s *Session) getWebhookSecret() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.webhookSecret
}

func (s *Session) getWebhookEvents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.webhookEvents
}

// webhookWants diz se o evento deve ser entregue conforme o filtro da sessão.
// Filtro vazio = entrega tudo (comportamento antigo).
func (s *Session) webhookWants(event string) bool {
	s.mu.Lock()
	ev := s.webhookEvents
	s.mu.Unlock()
	if len(ev) == 0 {
		return true
	}
	for _, e := range ev {
		if e == event {
			return true
		}
	}
	return false
}

func (s *Session) getWebhook() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.webhook
}

func (s *Session) setChatwoot(c ChatwootConfig) {
	s.mu.Lock()
	s.chatwoot = c
	s.mu.Unlock()
}

func (s *Session) getChatwoot() ChatwootConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chatwoot
}

func (s *Session) setRecording(on bool) {
	s.mu.Lock()
	s.recording = on
	s.mu.Unlock()
}

func (s *Session) getRecording() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recording
}

func (s *Session) setProxy(url string) {
	s.mu.Lock()
	s.proxy = url
	s.mu.Unlock()
}

func (s *Session) getProxy() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proxy
}

// applyProxy aplica o proxy configurado ao client whatsmeow. Precisa rodar ANTES
// do Connect(); trocar depois exige reconnect() (o whatsmeow só relê no dial).
func (s *Session) applyProxy() {
	addr := s.getProxy()
	if err := s.client.SetProxyAddress(addr); err != nil {
		s.log.Warn("proxy inválido, conectando sem proxy", "err", err)
	}
}

// reconnect derruba e reconecta a sessão pareada para o novo proxy valer.
func (s *Session) reconnect() {
	if s.client.Store.ID == nil {
		return // não pareada: o proxy será aplicado no próximo pareamento
	}
	s.client.Disconnect()
	s.applyProxy()
	if err := s.client.Connect(); err != nil {
		s.log.Error("reconexão após troca de proxy falhou", "err", err)
	}
}

func newSession(mgr *SessionManager, id, name string, client *whatsmeow.Client) *Session {
	s := &Session{
		id:     id,
		name:   name,
		mgr:    mgr,
		log:    mgr.log.With("session", id),
		client: client,
		auth:   AuthSnapshot{State: "connecting"},
		reg:    newCallRegistry(),
	}
	client.AddEventHandler(s.handleEvent)
	return s
}

// createCall monta a chamada. record liga a gravação nesta chamada específica —
// o chamador combina o opt-in por sessão (getRecording) com o flag por chamada.
func (s *Session) createCall(callID string, record bool) *call.CallManager {
	cm := call.NewCallManager(wa.NewSocket(s.client), s.log)
	s.wireCall(cm, callID)
	// [P3] canais criados ANTES do reg.add (happens-before via r.mu): fecha a janela em que
	// um teardown veria stopPlayout/playoutDone nil.
	ac := &activeCall{cm: cm, stopPlayout: make(chan struct{}), playoutDone: make(chan struct{})}
	if record {
		ac.recorder = newCallRecorder(callID, s.log, time.Now())
	}
	s.reg.add(callID, ac)
	s.startBrowserPlayout(callID, ac) // [pacing BR 12/08] loop de 20ms p/ o browser
	return cm
}

// [pacing BR 12/08] Playout paceado (leg WhatsApp->navegador): frames de 20ms (o que o
// WebRTC/NetEQ espera). [frota6c 06/10] O cushion fixo de 40ms + teto fixo de 200ms deu
// lugar ao buffer ADAPTATIVO de playout_buffer.go (alvo 60-400ms, descarte suave, teto
// duro 1000ms) — parâmetros e porquês lá.

// startBrowserPlayout dispara o loop que drena ac.playout a 20ms REGULARES e escreve Opus
// 20ms no navegador. É a correção do delay: absorve no MOTOR o jitter que a Meta entrega,
// em vez de repassá-lo ao Chrome (que inflava o buffer p/ ~1s). Silêncio no underrun mantém
// a cadência (e re-prime o cushion). O loop lê bridge/codec por reg.mediaOf a cada tick
// (enxerga swap de ponte). PARA no removeCall/drain ANTES de o codec cgo ser fechado.
func (s *Session) startBrowserPlayout(callID string, ac *activeCall) {
	stop := ac.stopPlayout // canais criados no literal do activeCall (createCall), antes do reg.add
	done := ac.playoutDone
	go func() {
		defer close(done)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		silence := make([]float32, playoutFrame48)
		// [drift 12/08] instrumentação: a cada 5s loga a TAXA de saída (frames*960/tempo) e a
		// de entrada (inSamples/tempo) contra o tempo REAL de parede. Se saída≈48000/s (tempo
		// real) o motor está correto e o drift do navegador é do lado do cliente. Se >48000/s,
		// o ticker/relogio do motor corre rápido. Um só ciclo de ligação já decide.
		var outFrames int64
		logStart := time.Now()
		var lastIn, lastDrop, lastUnder, lastTrim, lastHard, lastLate int64
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
			outFrames++
			if outFrames%250 == 0 { // ~5s
				now := time.Now()
				el := now.Sub(logStart).Seconds()
				ac.playoutMu.Lock()
				pb := &ac.playout
				in, dr, buf, tgt := pb.in, pb.dropped(), pb.level(), pb.targetMs()
				under, trim, hard := pb.underruns, pb.trimmed, pb.hardDropped
				ac.playoutMu.Unlock()
				late := ac.cm.PeerLateDrops() // atômico; cm é fixo desde o createCall
				if el > 0 {
					outSmpS := float64(250*playoutFrame48) / el
					inSmpS := float64(in-lastIn) / el
					// drop_delta = trimmed_delta + hard_drop_delta (amostras 48k), mantido p/
					// comparar com os logs anteriores (teto fixo de 200ms).
					s.log.Info("[voz-drift]",
						"out_smp_s", int(outSmpS), "out_x_realtime", outSmpS/48000,
						"in_smp_s", int(inSmpS), "in_x_realtime", inSmpS/48000,
						"buf_ms", buf/48, "drop_delta", dr-lastDrop,
						"target_ms", tgt, "underruns_delta", under-lastUnder,
						"trimmed_delta", trim-lastTrim, "hard_drop_delta", hard-lastHard,
						"late_pkts_delta", late-lastLate, "call_id", callID)
				}
				logStart, lastIn, lastDrop = now, in, dr
				lastUnder, lastTrim, lastHard, lastLate = under, trim, hard, late
			}
			// (1) DECIDE o frame de 20ms só com o buffer (playoutMu). O codec NÃO entra aqui.
			// emit=false só antes do 1º prime (acumulando o alvo inicial de 60ms).
			// [P2] Depois do 1º prime, underrun/re-prime devolvem frame nil = SILÊNCIO e a
			// cadência RTP de 20ms NUNCA para — evita o gap que faria o NetEQ do Chrome
			// re-inflar o buffer. O re-prime (segurar até o nível voltar ao alvo) é com
			// silêncio emitido, não com pausa de envio.
			ac.playoutMu.Lock()
			frame, emit := ac.playout.next()
			ac.playoutMu.Unlock()
			if !emit {
				continue
			}
			if frame == nil {
				frame = silence
			}

			// (2) SNAPSHOT + Encode + WriteOpus sob mediaMu — ATÔMICO vs o Close de oldOC/oldB
			// em setBridge. O tick roda inteiro ANTES do swap (codec antigo ainda válido) ou
			// inteiro DEPOIS (mediaOf devolve o novo). [P1] evita opus_encoder_destroy correndo
			// contra o Encode → segfault. Encode é cgo sub-ms; o Close espera no máx. 1 tick.
			ac.mediaMu.Lock()
			b, oc, ok := s.reg.mediaOf(callID)
			if ok && b != nil && oc != nil {
				if opus, err := oc.Encode(frame); err == nil && len(opus) > 0 {
					_ = b.WriteOpus(opus, 20*time.Millisecond)
				}
			}
			ac.mediaMu.Unlock()
			if !ok {
				return // chamada encerrada
			}
		}
	}()
}

// stopBrowserPlayout encerra o loop e ESPERA ele sair antes que o chamador feche o codec
// cgo (browserOpus). Sem essa barreira, o Encode do loop poderia rodar sobre um codec já
// liberado (use-after-free no cgo). Idempotente.
func stopBrowserPlayout(ac *activeCall) {
	if ac.stopPlayout == nil {
		return
	}
	close(ac.stopPlayout)
	<-ac.playoutDone
	ac.stopPlayout = nil
}

func (s *Session) wireCall(cm *call.CallManager, callID string) {
	cm.OnIncoming = func(c *call.CallInfo) {
		// numa chamada o PeerJid quase sempre vem como LID (123@lid); resolve pro
		// telefone real (PN) e, se der, pro nome do contato — senão a UI/widget
		// mostra o LID cru (issue #9).
		phone, name := s.resolvePeer(c.PeerJid)
		s.mgr.broker.upsertCall(CallRecord{
			SessionID: s.id, CallID: c.CallID, Direction: "inbound", Peer: c.PeerJid,
			StartedAt: time.Now().UnixMilli(), Status: StatusRinging,
		})
		s.mgr.broker.emitIncoming(s.id, c.CallID, c.PeerJid, phone, name, c.MediaType == core.CallMediaTypeVideo)
	}
	cm.OnStateChange = func(c *call.CallInfo) {
		if c.IsEnded() {
			s.removeCall(c.CallID)
			s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
			return
		}
		// [upstream 4301d8f] atendida: marca e cancela o timeout de toque (não expirar).
		// [frota6] Vale para QUALQUER estado fora do toque (connecting/active/on_hold), não
		// só "active": o upstream só marcava em active, e um atendimento aos ~59s ainda em
		// connecting (ICE/relay subindo) seria recusado pelo timer aos 60s.
		if !c.IsRinging() {
			if ac, ok := s.reg.get(c.CallID); ok {
				ac.answered.Store(true)
				s.reg.stopRingTimer(c.CallID)
			}
		}
		dir := "outbound"
		if c.Direction == core.CallDirectionIncoming {
			dir = "inbound"
		}
		existing, _ := s.mgr.broker.getCall(c.CallID)
		rec := CallRecord{
			SessionID: s.id, CallID: c.CallID, Direction: dir, Peer: c.PeerJid,
			StartedAt: time.Now().UnixMilli(), Status: mapStatus(c.StateData.State),
			Held: c.StateData.State == core.CallStateOnHold,
		}
		if existing != nil {
			rec.Owner = existing.Owner
			rec.StartedAt = existing.StartedAt
		}
		s.mgr.broker.upsertCall(rec)
	}
	cm.OnEnded = func(c *call.CallInfo) {
		s.removeCall(c.CallID)
		s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
	}
	cm.OnPeerAudio = func(pcm16 []float32) {
		ac, ok := s.reg.get(callID)
		if !ok {
			return
		}
		// grava o lado do peer (WhatsApp) mesmo se o navegador ainda não estiver pronto
		ac.recorder.writePeer(pcm16)
		if ac.bridge == nil || ac.browserOpus == nil {
			return
		}
		// [pacing BR 12/08] NÃO encoda+envia aqui. Antes, isto rodava na cadência de
		// CHEGADA da Meta (com o jitter dela) e em frames de 60ms → o NetEQ do Chrome
		// inflava o jitter buffer p/ ~1s. Agora empilha o PCM 48k; o loop startBrowserPlayout
		// drena a 20ms REGULARES, absorvendo o jitter no motor. [frota6c] O buffer é
		// adaptativo (playout_buffer.go): a chegada só enfileira; descarte (suave ou acima
		// do teto duro de 1000ms) é decidido no tick.
		pcm48 := media.Upsample16to48(pcm16)
		ac.playoutMu.Lock()
		ac.playout.push(pcm48)
		ac.playoutMu.Unlock()
	}
	cm.OnPeerVideo = func(au []byte) {
		ac, ok := s.reg.get(callID)
		if !ok || ac.bridge == nil {
			return
		}
		_ = ac.bridge.WriteVideo(au)
	}
	cm.OnVideoUpgradeRequest = func(c *call.CallInfo) {
		s.mgr.broker.emitVideoState(s.id, c.CallID, "upgrade-request", c.StateData.PeerVideoOn,
			!c.StateData.VideoOff, c.StateData.VideoUpgradeIncoming, c.StateData.VideoUpgradeOutgoing)
	}
	cm.OnVideoStateChanged = func(c *call.CallInfo) {
		s.mgr.broker.emitVideoState(s.id, c.CallID, "state", c.StateData.PeerVideoOn,
			!c.StateData.VideoOff, c.StateData.VideoUpgradeIncoming, c.StateData.VideoUpgradeOutgoing)
	}
}

func (s *Session) startOutgoing(ctx context.Context, peer types.JID, isVideo, record bool) (string, error) {
	callID := signaling.GenerateCallID()
	// grava se a sessão está em modo gravação OU se esta chamada pediu (flag record)
	cm := s.createCall(callID, record || s.getRecording())
	if err := cm.StartCall(ctx, callID, peer, isVideo); err != nil {
		s.removeCall(callID)
		return "", err
	}
	return callID, nil
}

// fakeCall dispara um toque fantasma (offer + terminate após dur) sem estabelecer
// mídia. Usa um CallManager transiente NÃO registrado no reg: é fire-and-forget e
// não deve ocupar o slot de chamada real da sessão.
func (s *Session) fakeCall(ctx context.Context, peer types.JID, isVideo bool, dur time.Duration) (string, error) {
	cm := call.NewCallManager(wa.NewSocket(s.client), s.log)
	return cm.FakeCall(ctx, peer, isVideo, dur)
}

func (s *Session) callForEvent(from types.JID, data *waBinary.Node) (*activeCall, bool) {
	callID := callIDFromNode(wrapCall(from, data))
	if callID == "" {
		return nil, false
	}
	return s.reg.get(callID)
}

// ringTimeoutSecs: segundos até expirar uma chamada de entrada que fica tocando
// e NUNCA recebe encerramento do WhatsApp (perdida/cancelada antes de conectar,
// queda de rede). Sem isso a chamada ficava pendurada pra sempre, entupindo o
// limite (WACALLS_MAX_CALLS) e fazendo novas chamadas serem recusadas sozinhas —
// só um restart do processo liberava. 0 desliga o timeout.
var ringTimeoutSecs = envInt("WACALLS_RING_TIMEOUT_SECONDS", 60)

func (s *Session) onIncomingOffer(ctx context.Context, evt *events.CallOffer) {
	node := wrapCall(evt.From, evt.Data)
	callID := callIDFromNode(node)
	if callID == "" {
		return
	}
	if max := s.mgr.maxCalls; max > 0 && s.reg.count() >= max {
		s.rejectOffer(ctx, node, evt.From)
		return
	}
	cm := s.createCall(callID, s.getRecording())
	cm.HandleCallOffer(ctx, node, evt.From)
	s.armRingTimeout(callID)
}

// armRingTimeout agenda a expiração da chamada tocando (ver ringTimeoutSecs). É
// cancelado quando a chamada é atendida (OnStateChange ativo) ou encerra
// (removeCall/drain param o timer).
func (s *Session) armRingTimeout(callID string) {
	if ringTimeoutSecs <= 0 {
		return
	}
	t := time.AfterFunc(time.Duration(ringTimeoutSecs)*time.Second, func() {
		s.expireRingingCall(callID)
	})
	s.reg.setRingTimer(callID, t)
}

// expireRingingCall encerra uma chamada que ficou tocando além do timeout sem
// nunca ter sido atendida nem recebido encerramento. Recusa no WhatsApp, avisa
// TODOS os assinantes (call-ended) e libera a vaga.
func (s *Session) expireRingingCall(callID string) {
	ac, ok := s.reg.get(callID)
	if !ok {
		return // já encerrou normalmente
	}
	if ac.answered.Load() {
		return // atendida (corrida com o timer): não expira
	}
	s.log.Info("chamada expirada por timeout de toque (nunca recebeu encerramento do WhatsApp)",
		"call_id", callID, "timeout_s", ringTimeoutSecs)
	_ = ac.cm.RejectCall(s.mgr.appCtx, callID, core.EndCallReasonTimeout)
	s.removeCall(callID)
	s.mgr.broker.endCall(callID, string(core.EndCallReasonTimeout))
}

func (s *Session) rejectOffer(ctx context.Context, node *waBinary.Node, from types.JID) {
	info := signaling.ExtractNodeInfo(node)
	if info == nil {
		return
	}
	creator := wanode.AttrString(info.InnerNode.Attrs, "call-creator")
	if creator == "" {
		creator = from.String()
	}
	sock := wa.NewSocket(s.client)
	own := sock.OwnLID()
	if from.Server == types.DefaultUserServer {
		own = sock.OwnPN()
	}
	reject := signaling.BuildRejectStanza(from, info.CallID, wanode.MustJID(creator), own)
	_ = sock.SendNode(ctx, reject)
	s.log.Info("inbound call rejected: session at capacity", "call_id", info.CallID)
}

// maybeMarkRead confirma leitura automática das mensagens recebidas quando
// read_messages está ligado na config do Chatwoot (paridade com a Evolution).
func (s *Session) maybeMarkRead(ctx context.Context, evt *events.Message) {
	if !s.getChatwoot().ReadMessages || evt.Info.IsFromMe {
		return
	}
	// não faz sentido/formato p/ newsletters e status/broadcast
	if evt.Info.Chat.Server == types.NewsletterServer || evt.Info.Chat.Server == types.BroadcastServer {
		return
	}
	go func() {
		err := s.client.MarkRead(ctx, []types.MessageID{evt.Info.ID}, evt.Info.Timestamp, evt.Info.Chat, evt.Info.Sender)
		if err != nil {
			s.log.Debug("read_messages: mark read failed", "err", err)
		}
	}()
}

func (s *Session) handleEvent(rawEvt any) {
	ctx := context.Background()
	switch evt := rawEvt.(type) {
	case *events.Connected:
		if id := s.client.Store.ID; id != nil {
			_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
		}
		s.setAuth(AuthSnapshot{State: "open", Paired: true})
		// reconectou: rearma o aviso de queda para a próxima desconexão real.
		s.mu.Lock()
		s.downAlerted = false
		s.mu.Unlock()
		// always_online: mantém a presença sempre disponível (reenvia a cada reconexão).
		if s.getChatwoot().AlwaysOnline {
			go func() { _ = s.client.SendPresence(ctx, types.PresenceAvailable) }()
		}
	case *events.Disconnected:
		// socket caiu; o whatsmeow reconecta sozinho e o Connected devolve "open".
		// Antes o estado seguia "open" e o painel mostrava conectado sem conexão.
		if s.client.Store.ID != nil {
			s.log.Warn("socket do WhatsApp caiu; reconectando")
			s.setAuth(AuthSnapshot{State: "connecting", Paired: true})
		}
	case *events.KeepAliveTimeout:
		s.log.Warn("WhatsApp sem resposta ao keepalive", "falhas", evt.ErrorCount)
		if evt.ErrorCount >= 2 && s.client.Store.ID != nil {
			s.setAuth(AuthSnapshot{State: "connecting", Paired: true})
		}
	case *events.KeepAliveRestored:
		if s.client.IsConnected() && s.client.Store.ID != nil {
			s.setAuth(AuthSnapshot{State: "open", Paired: true})
		}
	case *events.PairSuccess:
		s.log.Info("pair-success recebido", "jid", evt.ID.String(), "plataforma", evt.Platform)
	case *events.PairError:
		s.log.Warn("pareamento falhou", "jid", evt.ID.String(), "err", evt.Error)
	case *events.ConnectFailure:
		s.log.Warn("WhatsApp recusou a conexão", "reason", evt.Reason.String(), "msg", evt.Message)
	case *events.LoggedOut:
		s.log.Warn("aparelho desvinculado pelo celular", "reason", evt.Reason.String())
		s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
		go s.notifyDisconnected("desconectada (o aparelho desvinculou este dispositivo)",
			"Reconecte lendo o QR no painel do AstraCalls para voltar a enviar e receber.")
	case *events.StreamReplaced:
		// o whatsmeow NÃO reconecta depois de stream replaced: o socket fica fechado.
		s.log.Warn("conexão substituída por outra do mesmo aparelho")
		s.setAuth(AuthSnapshot{State: "replaced", Paired: true})
		go s.notifyDisconnected("substituída por outra conexão do mesmo número",
			"Se não foi proposital, reconecte pelo painel do AstraCalls.")
	case *events.TemporaryBan:
		go s.notifyDisconnected("bloqueada temporariamente pelo WhatsApp ("+evt.String()+")",
			"Aguarde o fim do bloqueio e evite disparos em massa.")
	case *events.ClientOutdated:
		go s.notifyDisconnected("recusada pelo WhatsApp: cliente desatualizado",
			"É necessário atualizar o AstraCalls. Avise o suporte técnico.")
	case *events.Message:
		switch {
		case evt.Message.GetPollUpdateMessage() != nil:
			go s.handleIncomingPollVote(evt) // voto em enquete (decodifica + encaminha)
		case evt.Message.GetEncEventResponseMessage() != nil:
			go s.handleIncomingEventResponse(evt) // RSVP de evento (decodifica + encaminha)
		case evt.Message.GetReactionMessage() != nil || evt.Message.GetEncReactionMessage() != nil:
			go s.handleIncomingReaction(evt) // reação (emoji) numa mensagem
		default:
			s.storeMessageEvent(evt)
			s.dispatchWebhook("message", summarizeMessage(evt))
			go s.chatwootPushIncoming(evt)
			s.maybeMarkRead(ctx, evt)
		}
	case *events.HistorySync:
		// conversas antigas que o WhatsApp envia ao parear -> importa pro Chatwoot
		go s.importHistorySync(evt.Data)
	case *events.GroupInfo:
		// entrou/saiu/promoveu no grupo -> webhook + nota no Chatwoot
		go s.handleGroupSystemEvent(evt)
	case *events.UndecryptableMessage:
		// visualização única chega como placeholder "unavailable" — o WhatsApp não
		// libera o conteúdo p/ dispositivos vinculados. Avisa o atendente/webhook.
		if evt.IsUnavailable && evt.UnavailableType == events.UnavailableTypeViewOnce {
			go s.handleUnavailableViewOnce(evt)
		}
	case *events.Receipt:
		s.dispatchWebhook("receipt", map[string]any{
			"chat": evt.Chat.String(), "sender": evt.Sender.String(),
			"type": string(evt.Type), "ids": evt.MessageIDs,
			"timestamp": evt.Timestamp.UnixMilli(),
		})
	case *events.CallOffer:
		s.onIncomingOffer(ctx, evt)
	case *events.CallAccept:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallAccept(ctx, wrapCall(evt.From, evt.Data), evt.From)
		}
	case *events.CallTransport:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTransport(ctx, wrapCall(evt.From, evt.Data), evt.From)
		}
	case *events.CallTerminate:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data))
		}
	case *events.CallReject:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data))
		}
	case *events.UnknownCallEvent:
		// stanzas de call sem evento tipado no whatsmeow — hoje o que nos interessa é
		// o <video state=N> de upgrade/downgrade mid-call.
		s.handleUnknownCall(ctx, evt)
	}
}

func (s *Session) handleUnknownCall(ctx context.Context, evt *events.UnknownCallEvent) {
	if evt.Node == nil {
		return
	}
	callID := callIDFromNode(evt.Node)
	if callID == "" {
		return
	}
	ac, ok := s.reg.get(callID)
	if !ok {
		return
	}
	// O whatsmeow só emite CallTerminate/CallReject tipado quando o <call> tem UM
	// filho só; quando a chamada é atendida/encerrada em outro device (ex.: o
	// celular), o WhatsApp às vezes manda o <terminate>/<reject> junto com outros
	// nós (relaylatency etc.), e isso cai aqui como UnknownCallEvent — antes era
	// ignorado, então a chamada nunca encerrava (ringtone preso nos atendentes e
	// vaga presa no limite de chamadas). Agora tratamos como terminal.
	if nodeHasTerminalCall(evt.Node) {
		s.log.Info("call encerrada em outro device (terminal via UnknownCallEvent)", "call_id", callID)
		ac.cm.HandleCallTerminate(evt.Node) // → OnEnded → broker.endCall (avisa TODOS) + removeCall
		return
	}
	ac.cm.HandleVideoState(ctx, evt.Node)
}

// nodeHasTerminalCall diz se um <call> traz um <terminate> ou <reject> entre os
// filhos (a chamada saiu do estado tocando: atendida/recusada/encerrada alhures).
func nodeHasTerminalCall(node *waBinary.Node) bool {
	for _, c := range node.GetChildren() {
		if c.Tag == "terminate" || c.Tag == "reject" {
			return true
		}
	}
	return false
}

func (s *Session) connect(ctx context.Context) error {
	s.applyProxy()
	if s.client.Store.ID != nil {
		return s.client.Connect()
	}
	return s.startPairing(ctx)
}

func (s *Session) startPairing(ctx context.Context) error {
	return s.pairLoop(ctx, true, nil)
}

// pairLoop conecta um device novo e trata os eventos do canal de pareamento.
// showQR=false é o pareamento por código: o QR que o servidor gera continua
// chegando, mas não é publicado. ready (se não nil) é fechado no primeiro evento,
// que é quando o whatsmeow diz que a conexão está pronta para pedir o código.
func (s *Session) pairLoop(ctx context.Context, showQR bool, ready chan<- struct{}) error {
	s.applyProxy()
	qrChan, err := s.client.GetQRChannel(ctx)
	if err != nil {
		return err
	}
	if err := s.client.Connect(); err != nil {
		return err
	}
	go func() {
		for evt := range qrChan {
			if ready != nil {
				close(ready)
				ready = nil
			}
			switch evt.Event {
			case "code":
				if !showQR {
					continue
				}
				s.log.Info("scan the QR code to pair this session")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				s.setAuth(AuthSnapshot{State: "qr", QR: evt.Code})
				s.mgr.broker.emitSessionQR(s.id, evt.Code)
			case "success":
				if id := s.client.Store.ID; id != nil {
					_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
				}
				s.log.Info("QR lido: pareado no servidor, aguardando o celular concluir o vínculo", "jid", jidOf(s.client))
				s.setAuth(AuthSnapshot{State: "open", Paired: true})
				go s.watchLink(s.client)
			case "timeout":
				s.log.Warn("pareamento: QR expirou sem leitura")
				s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
			case "passkey-request":
				// conta com passkey: o WhatsApp exige uma prova WebAuthn do dono.
				// Expõe o desafio pro front (que delega ao autenticador via extensão)
				// e recebe a assinatura de volta em POST .../pair-passkey.
				s.setPasskeyChallenge(evt.PasskeyRequest.PublicKey)
			case "passkey-confirmation":
				// handoff manual: confirma o código exibido no WhatsApp do dono.
				if err := s.client.SendPasskeyConfirmation(s.mgr.appCtx); err != nil {
					s.log.Warn("passkey: confirmação falhou", "err", err)
				}
			case "error":
				s.log.Warn("pareamento: erro", "err", evt.Error)
			}
		}
	}()
	return nil
}

// setPasskeyChallenge serializa o desafio WebAuthn e o publica no estado de auth
// (via SSE), no mesmo modelo do QR. O front repassa esse objeto ao autenticador
// (navigator.credentials.get) na origem web.whatsapp.com através da extensão.
func (s *Session) setPasskeyChallenge(pk *types.WebAuthnPublicKey) {
	if pk == nil {
		return
	}
	raw, err := json.Marshal(pk)
	if err != nil {
		s.log.Warn("passkey: falha ao serializar desafio", "err", err)
		return
	}
	s.setAuth(AuthSnapshot{State: "passkey_request", Passkey: raw})
}

// startPhonePairing conecta um device novo e solicita um código de pareamento
// por telefone (o usuário digita o código no WhatsApp: Aparelhos conectados ->
// Conectar com número). O sucesso chega depois via events.Connected.
func (s *Session) startPhonePairing(ctx context.Context, phone string) (string, error) {
	// O whatsmeow exige conexão pronta (primeiro evento do canal) antes do
	// PairPhone; pedir logo após o Connect dava 400 bad-request.
	ready := make(chan struct{})
	if err := s.pairLoop(ctx, false, ready); err != nil {
		return "", err
	}
	select {
	case <-ready:
	case <-time.After(15 * time.Second):
		return "", errors.New("conexão com o WhatsApp não ficou pronta para gerar o código")
	}
	// O nome precisa ser "Navegador (SO)" com valores conhecidos; "AstraCalls"
	// sempre voltava 400 bad-request.
	code, err := s.client.PairPhone(ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		return "", err
	}
	s.setAuth(AuthSnapshot{State: "pairing_code", Code: code})
	return code, nil
}

func (s *Session) setAuth(a AuthSnapshot) {
	s.mu.Lock()
	s.auth = a
	s.mu.Unlock()
	s.mgr.broker.emitAuthState(s.id, a)
	s.mgr.broker.emitSessionList(s.mgr.infos())
}

// notifyDisconnected avisa — UMA vez por queda — que a sessão do WhatsApp caiu:
// dispara o webhook "session_status" e posta um alerta na inbox do Chatwoot para
// o atendente ver que precisa reconectar. reason descreve o que houve; action é a
// instrução. O guard downAlerted (resetado em events.Connected) evita repetição.
func (s *Session) notifyDisconnected(reason, action string) {
	s.mu.Lock()
	if s.downAlerted {
		s.mu.Unlock()
		return
	}
	s.downAlerted = true
	s.mu.Unlock()

	s.log.Warn("sessão do WhatsApp desconectada — avisando", "reason", reason)
	s.dispatchWebhook("session_status", map[string]any{
		"status": "disconnected", "reason": reason, "session": s.id, "name": s.name,
	})
	text := "⚠️ *AstraCalls — WhatsApp desconectado*\n" +
		"A sessão *" + s.name + "* foi " + reason + ".\n" + action
	s.chatwootAlert(text)
}

func (s *Session) info() SessionInfo {
	s.mu.Lock()
	a := s.auth
	rec := s.recording
	s.mu.Unlock()
	jid := ""
	if id := s.client.Store.ID; id != nil {
		jid = id.String()
	}
	state := a.State
	linked := jid != "" && s.linkDone()
	if state == "open" && !linked {
		// pareada no servidor, mas o celular ainda não concluiu o vínculo.
		state = "linking"
	}
	return SessionInfo{ID: s.id, Name: s.name, JID: jid, State: state, Paired: a.Paired || jid != "", Linked: linked, Recording: rec}
}

// linkDone informa se o celular já entregou a chave de app state (vínculo
// concluído). Consulta o store só enquanto ainda não viu a chave; depois fica
// em memória até o próximo pareamento.
func (s *Session) linkDone() bool {
	s.mu.Lock()
	done := s.linked
	s.mu.Unlock()
	if done {
		return true
	}
	ctx, cancel := context.WithTimeout(s.mgr.appCtx, 2*time.Second)
	defer cancel()
	id, err := s.client.Store.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
	if err != nil || id == nil {
		return false
	}
	s.mu.Lock()
	s.linked = true
	s.mu.Unlock()
	return true
}

// watchLink acompanha o fim do vínculo depois do pair-success: avisa a lista de
// sessões assim que o celular entrega a chave (o painel sai de "linking" sem
// esperar outro evento) e registra quando o celular não conclui a tempo.
func (s *Session) watchLink(client *whatsmeow.Client) {
	const limite = 5 * time.Minute
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	fim := time.After(limite)
	for {
		select {
		case <-s.mgr.appCtx.Done():
			return
		case <-fim:
			if s.client == client && !s.linkDone() {
				s.log.Warn("vínculo incompleto: o celular não concluiu em 5 min; desvincule este aparelho no celular e pareie de novo",
					"jid", jidOf(client))
			}
			return
		case <-tick.C:
			if s.client != client {
				return // outro pareamento começou
			}
			if s.linkDone() {
				s.log.Info("vínculo concluído pelo celular", "jid", jidOf(client))
				s.mgr.broker.emitSessionList(s.mgr.infos())
				return
			}
		}
	}
}

func jidOf(c *whatsmeow.Client) string {
	if id := c.Store.ID; id != nil {
		return id.String()
	}
	return ""
}

func (s *Session) setBridge(callID string, b *Bridge, oc media.Codec) {
	ac, ok := s.reg.get(callID)
	if !ok {
		b.Close()
		if oc != nil {
			oc.Close()
		}
		return
	}
	// [P1 12/08] Segura o mediaMu em volta do swap E do Close de oldB/oldOC: o loop de playout
	// usa o codec/ponte sob o MESMO mutex, então nunca há Encode/WriteOpus rodando enquanto
	// opus_encoder_destroy/PeerConnection.Close executam (senão = use-after-free/segfault).
	// oldB.DisableTerminate() antes do Close impede que fechar a ponte antiga encerre a chamada.
	ac.mediaMu.Lock()
	defer ac.mediaMu.Unlock()
	oldB, oldOC, found := s.reg.setBridge(callID, b, oc)
	if !found {
		b.Close()
		if oc != nil {
			oc.Close()
		}
		return
	}
	if oldB != nil {
		oldB.DisableTerminate()
		oldB.Close()
	}
	if oldOC != nil {
		oldOC.Close()
	}
}

func (s *Session) removeCall(callID string) {
	ac, ok := s.reg.remove(callID)
	if !ok {
		return
	}
	stopBrowserPlayout(ac) // [pacing BR 12/08] para o loop ANTES de fechar o codec cgo
	s.finalizeRecording(ac)
	if ac.bridge != nil {
		ac.bridge.Close()
	}
	if ac.wsBridge != nil {
		ac.wsBridge.Close()
	}
	if ac.browserOpus != nil {
		ac.browserOpus.Close()
	}
}

// finalizeRecording encerra a gravação (encode MP3) e entrega o áudio (Chatwoot
// + webhook). Roda em goroutine pois o encode (ffmpeg) é lento e não pode segurar
// o teardown. finish() é idempotente, então é seguro chamar pelos dois caminhos
// de término (removeCall / teardownAllCalls).
func (s *Session) finalizeRecording(ac *activeCall) {
	if ac == nil || ac.recorder == nil {
		return
	}
	rec := ac.recorder
	callID := rec.callID
	peer := ""
	if cr, ok := s.mgr.broker.getCall(callID); ok && cr != nil {
		peer = cr.Peer
	}
	go func() {
		path, seconds, ok := rec.finish()
		if !ok {
			return
		}
		s.onRecordingReady(callID, peer, path, seconds)
	}()
}

func (s *Session) terminateCall(callID string, reason core.EndCallReason) {
	ac, ok := s.reg.get(callID)
	if !ok {
		return
	}
	_ = ac.cm.EndCall(context.Background(), reason)
}

func (s *Session) teardownAllCalls() {
	for _, ac := range s.reg.drain() {
		_ = ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
		stopBrowserPlayout(ac) // [pacing BR 12/08] para o loop ANTES de fechar o codec cgo
		s.finalizeRecording(ac)
		if ac.bridge != nil {
			ac.bridge.Close()
		}
		if ac.wsBridge != nil {
			ac.wsBridge.Close()
		}
		if ac.browserOpus != nil {
			ac.browserOpus.Close()
		}
	}
}

// setWSBridge registra uma ponte WebSocket na chamada ativa. Usa o mesmo
// mecanismo de setBridge: fecha a ponte anterior (WebRTC ou WS) sem disparar
// o encerramento da chamada (DisableTerminate).
func (s *Session) setWSBridge(callID string, b *wsBridge, oc media.Codec) {
	oldWSB, oldB, oldOC, found := s.reg.setWSBridge(callID, b, oc)
	if !found {
		b.Close()
		if oc != nil {
			oc.Close()
		}
		return
	}
	if oldB != nil {
		oldB.DisableTerminate()
		oldB.Close()
	}
	if oldWSB != nil {
		oldWSB.DisableTerminate()
		oldWSB.Close()
	}
	if oldOC != nil {
		oldOC.Close()
	}
}

func (s *Session) replaceClient(client *whatsmeow.Client) {
	s.teardownAllCalls()
	s.client.Disconnect()
	s.client = client
	s.mu.Lock()
	s.linked = false
	s.mu.Unlock()
	client.AddEventHandler(s.handleEvent)
}

func (s *Session) shutdown() {
	s.teardownAllCalls()
	s.client.Disconnect()
	if s.waDB != nil {
		_ = s.waDB.Close()
	}
}

func mapStatus(state core.CallState) CallStatus {
	switch state {
	case core.CallStateActive, core.CallStateOnHold:
		return StatusConnected // em espera ainda é uma chamada conectada (flag held à parte)
	case core.CallStateEnded:
		return StatusEnded
	case core.CallStateInitiating:
		return StatusStarting
	default:
		return StatusRinging
	}
}
