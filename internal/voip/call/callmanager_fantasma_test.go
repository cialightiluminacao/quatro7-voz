package call

// Testes do fix "chamadas-fantasma" (caso Isadora 17/08): chamada RECEBIDA atendida
// no CELULAR (outro device da conta) deixava o leg do motor "ativo" p/ sempre — o
// accept do irmão não encerrava e o terminate do cliente era descartado pelo guard.

import (
	"context"
	"log/slog"
	"testing"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/wanode"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

// incomingManager monta um CallManager com chamada RECEBIDA tocando (IncomingRinging).
// Conta própria = fakeSock (OwnLID 999@lid, OwnPN 999@s.whatsapp.net).
func incomingManager(sock core.VoipSocket) *CallManager {
	m := NewCallManager(sock, slog.Default())
	m.relay = fakeRelay{}
	call := NewIncomingCall("CALL1", "271592307339424@lid", "271592307339424@lid", "554899542409", core.CallMediaTypeAudio)
	call.EncryptionKey = make([]byte, 32)
	m.currentCall = call
	return m
}

func rejectNode(callID string, from types.JID) *waBinary.Node {
	return &waBinary.Node{
		Tag:   "call",
		Attrs: waBinary.Attrs{"from": from},
		Content: []waBinary.Node{{
			Tag:   "reject",
			Attrs: waBinary.Attrs{"call-id": callID, "call-creator": "271592307339424@lid"},
		}},
	}
}

// Atendeu no CELULAR (irmão da NOSSA conta) com o motor ainda tocando → o leg do
// motor encerra na hora com accepted_elsewhere (era o nascedouro do fantasma).
func TestIrmaoAtendeEncerraLegDoMotor(t *testing.T) {
	sock := &fakeSock{}
	m := incomingManager(sock)
	ended := 0
	m.OnEnded = func(*CallInfo) { ended++ }

	celular := types.NewJID("999", types.HiddenUserServer) // device 0? não: o nosso fake é 999@lid…
	celular.Device = 5                                     // …então o IRMÃO precisa divergir no String()
	m.HandleCallAccept(context.Background(), acceptNode("CALL1", celular), celular)

	if !m.currentCall.IsEnded() {
		t.Fatalf("leg do motor devia encerrar; estado=%s", m.currentCall.StateData.State)
	}
	if m.currentCall.StateData.EndReason != core.EndCallReasonAcceptedElsewhere {
		t.Fatalf("reason=%s, quer accepted_elsewhere", m.currentCall.StateData.EndReason)
	}
	if ended != 1 {
		t.Fatalf("OnEnded disparou %d vezes, quer 1", ended)
	}
	// retransmissão do MESMO accept: não re-encerra nem dispara OnEnded de novo
	m.HandleCallAccept(context.Background(), acceptNode("CALL1", celular), celular)
	if ended != 1 {
		t.Fatalf("retransmissão re-disparou OnEnded (%d)", ended)
	}
}

// Dual-accept: o operador JÁ atendeu no softphone (Connecting) e o accept atrasado
// do celular chega → ignorar (first local accept wins), a chamada NÃO cai.
func TestIrmaoAcceptIgnoradoAposAtenderLocal(t *testing.T) {
	sock := &fakeSock{}
	m := incomingManager(sock)
	if err := m.currentCall.ApplyTransition(Transition{Type: TransitionLocalAccepted}); err != nil {
		t.Fatalf("local accept: %v", err)
	}
	celular := types.NewJID("999", types.HiddenUserServer)
	celular.Device = 5
	m.HandleCallAccept(context.Background(), acceptNode("CALL1", celular), celular)
	if m.currentCall.IsEnded() {
		t.Fatal("accept atrasado do irmão derrubou a chamada atendida localmente")
	}
	if m.acceptedByJid != "" {
		t.Fatalf("acceptedByJid não podia ser tomado pelo irmão: %q", m.acceptedByJid)
	}
}

func TestAcceptedElsewhereAfterLocalAcceptKeepsActiveMotorLeg(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "on hold"}[held], func(t *testing.T) {
			m := incomingManager(&fakeSock{})
			t.Cleanup(m.cleanupMedia)
			ended := 0
			m.OnEnded = func(*CallInfo) { ended++ }
			if err := m.AcceptCall(context.Background(), "CALL1"); err != nil {
				t.Fatal(err)
			}
			if err := m.currentCall.ApplyTransition(Transition{Type: TransitionMediaConnected}); err != nil {
				t.Fatal(err)
			}
			if held {
				if err := m.Hold(false); err != nil {
					t.Fatal(err)
				}
			}
			state := m.currentCall.StateData.State
			caller := wanode.MustJID(m.currentCall.PeerJid)
			node := terminateNode("CALL1", caller)
			node.GetChildren()[0].Attrs["reason"] = "accepted_elsewhere"
			m.HandleCallTerminate(node)
			m.HandleCallTerminate(node) // retry também não limpa mídia nem chama OnEnded
			if m.currentCall.StateData.State != state || ended != 0 {
				t.Fatalf("accepted_elsewhere derrubou leg local ativo: state=%s ended=%d", m.currentCall.StateData.State, ended)
			}
			m.HandleCallTerminate(terminateNode("CALL1", caller))
			if !m.currentCall.IsEnded() || ended != 1 {
				t.Fatalf("desligamento normal deve encerrar: state=%s ended=%d", m.currentCall.StateData.State, ended)
			}
		})
	}
}

func TestAcceptedElsewhereGuardRequiresIncomingLocalAcceptAndActiveMedia(t *testing.T) {
	cases := []struct {
		name      string
		outgoing  bool
		local     bool
		connected bool
	}{
		{name: "incoming still ringing"},
		{name: "incoming local accept still connecting", local: true},
		{name: "outgoing active", outgoing: true, connected: true},
		{name: "incoming active without local accept", connected: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			m := incomingManager(&fakeSock{})
			if tt.outgoing {
				m.currentCall.Direction = core.CallDirectionOutgoing
			}
			if tt.local {
				if err := m.currentCall.ApplyTransition(Transition{Type: TransitionLocalAccepted}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.connected {
				m.currentCall.StateData.State = core.CallStateActive
			}
			node := terminateNode("CALL1", wanode.MustJID(m.currentCall.PeerJid))
			node.GetChildren()[0].Attrs["reason"] = "accepted_elsewhere"
			m.HandleCallTerminate(node)
			if !m.currentCall.IsEnded() || m.currentCall.StateData.EndReason != core.EndCallReasonAcceptedElsewhere {
				t.Fatalf("guard amplo demais: state=%s reason=%s", m.currentCall.StateData.State, m.currentCall.StateData.EndReason)
			}
		})
	}
}

// Depois do celular atender, o terminate do CLIENTE tem de ser processado (era o
// que o guard antigo descartava como "non-answering device" → fantasma eterno).
func TestTerminateDoClienteProcessaAposIrmaoAtender(t *testing.T) {
	sock := &fakeSock{}
	m := incomingManager(sock)
	celular := types.NewJID("999", types.HiddenUserServer)
	celular.Device = 5
	m.HandleCallAccept(context.Background(), acceptNode("CALL1", celular), celular)
	// leg já encerrado; o terminate do cliente não pode panicar nem "ressuscitar" nada
	cliente := types.NewJID("271592307339424", types.HiddenUserServer)
	m.HandleCallTerminate(terminateNode("CALL1", cliente))
	if !m.currentCall.IsEnded() {
		t.Fatal("chamada devia permanecer encerrada")
	}
}

// Bug ORIGINAL do Eduardo: atender no softphone e o "dispensar" (reject tardio) do
// celular derrubava a conversa em curso. Reject só derruba chamada que ainda TOCA.
func TestRejectTardioDoCelularNaoDerrubaChamadaAtendida(t *testing.T) {
	sock := &fakeSock{}
	m := incomingManager(sock)
	if err := m.currentCall.ApplyTransition(Transition{Type: TransitionLocalAccepted}); err != nil {
		t.Fatalf("local accept: %v", err)
	}
	celular := types.NewJID("999", types.HiddenUserServer)
	celular.Device = 5
	m.HandleCallTerminate(rejectNode("CALL1", celular))
	if m.currentCall.IsEnded() {
		t.Fatal("reject tardio do celular derrubou a chamada atendida no softphone")
	}
}

// Recusar no celular COM a chamada ainda tocando segue derrubando em todo lugar
// (comportamento legítimo do WhatsApp que não podemos regredir).
func TestRejectComChamadaTocandoSegueValendo(t *testing.T) {
	sock := &fakeSock{}
	m := incomingManager(sock)
	celular := types.NewJID("999", types.HiddenUserServer)
	celular.Device = 5
	m.HandleCallTerminate(rejectNode("CALL1", celular))
	if !m.currentCall.IsEnded() {
		t.Fatal("reject com chamada tocando devia encerrar")
	}
}

// Cliente desiste enquanto toca (terminate do caller, ninguém atendeu) — pré-existente.
func TestTerminateDoCallerEnquantoToca(t *testing.T) {
	sock := &fakeSock{}
	m := incomingManager(sock)
	cliente := types.NewJID("271592307339424", types.HiddenUserServer)
	m.HandleCallTerminate(terminateNode("CALL1", cliente))
	if !m.currentCall.IsEnded() {
		t.Fatal("terminate do caller devia encerrar a chamada tocando")
	}
}

// [frota6] Ordem dos guards em HandleCallTerminate após o merge com o upstream: o
// uncallable de um irmão @hosted.lid (upstream c6073d6) é ignorado com a chamada
// tocando, e um reject comum do celular (nosso guard) continua derrubando.
func TestHostedLIDUncallableIgnoradoMasRejectDoCelularDerruba(t *testing.T) {
	m := incomingManager(&fakeSock{})
	hosted := &waBinary.Node{
		Tag:   "call",
		Attrs: waBinary.Attrs{"from": "999:99@hosted.lid"},
		Content: []waBinary.Node{{
			Tag:   "reject",
			Attrs: waBinary.Attrs{"call-id": "CALL1", "call-creator": "271592307339424@lid", "reason": "uncallable"},
		}},
	}
	m.HandleCallTerminate(hosted)
	if m.currentCall.IsEnded() {
		t.Fatal("uncallable do irmão hosted.lid não pode derrubar a chamada tocando")
	}
	celular := types.NewJID("999", types.HiddenUserServer)
	celular.Device = 5
	m.HandleCallTerminate(rejectNode("CALL1", celular))
	if !m.currentCall.IsEnded() {
		t.Fatal("reject do celular com a chamada tocando devia encerrar")
	}
}

// [frota6] Terminate que chega como UnknownCallEvent (vários filhos, o terminal não é
// o primeiro — upstream 2998176) vindo do CLIENTE com a chamada tocando encerra.
func TestTerminateMultiNoDoClienteEncerra(t *testing.T) {
	m := incomingManager(&fakeSock{})
	n := &waBinary.Node{
		Tag:   "call",
		Attrs: waBinary.Attrs{"from": "271592307339424@lid"},
		Content: []waBinary.Node{
			{Tag: "relaylatency", Attrs: waBinary.Attrs{"call-id": "CALL1"}},
			{Tag: "terminate", Attrs: waBinary.Attrs{"call-id": "CALL1", "call-creator": "271592307339424@lid"}},
		},
	}
	m.HandleCallTerminate(n)
	if !m.currentCall.IsEnded() {
		t.Fatal("terminate multi-nó do cliente devia encerrar a chamada tocando")
	}
}
