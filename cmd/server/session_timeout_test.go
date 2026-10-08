package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

type timeoutTestSocket struct {
	core.VoipSocket
	rejects atomic.Int64
}

func (*timeoutTestSocket) OwnLID() types.JID { return types.NewJID("999", types.HiddenUserServer) }
func (*timeoutTestSocket) OwnPN() types.JID  { return types.NewJID("999", types.DefaultUserServer) }
func (sock *timeoutTestSocket) SendNode(_ context.Context, n waBinary.Node) error {
	for _, child := range n.GetChildren() {
		if child.Tag == "reject" {
			sock.rejects.Add(1)
		}
	}
	return nil
}

func (sock *timeoutTestSocket) Query(ctx context.Context, n waBinary.Node) (*waBinary.Node, error) {
	return nil, sock.SendNode(ctx, n)
}

func ringingTimeoutSession() (*Session, *activeCall) {
	return ringingTimeoutSessionWithSocket(&timeoutTestSocket{})
}

func ringingTimeoutSessionWithSocket(sock core.VoipSocket) (*Session, *activeCall) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &Session{log: log, reg: newCallRegistry(), mgr: &SessionManager{appCtx: context.Background(), broker: NewBroker()}}
	cm := call.NewCallManager(sock, log)
	peer := types.NewJID("123", types.HiddenUserServer)
	cm.HandleCallOffer(context.Background(), &waBinary.Node{Tag: "call", Content: []waBinary.Node{
		{Tag: "offer", Attrs: waBinary.Attrs{"call-id": "c1", "call-creator": peer.String()}},
	}}, peer)
	ac := &activeCall{cm: cm, browserOpus: &browserTestCodec{}}
	s.reg.add("c1", ac)
	s.mgr.broker.upsertCall(CallRecord{CallID: "c1", Status: StatusRinging})
	return s, ac
}

func TestExpireRingingCallHonorsAcceptanceEvenWithoutAnsweredFlag(t *testing.T) {
	s, ac := ringingTimeoutSession()
	defer ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
	defer s.removeCall("c1")
	if err := ac.cm.AcceptCall(context.Background(), "c1"); err != nil {
		t.Fatal(err)
	}
	// Simula Load(answered)==false seguido de aceite antes de RejectCall.
	if ac.answered.Load() {
		t.Fatal("teste deve manter flag answered falsa")
	}
	s.expireRingingCall("c1")
	if _, ok := s.reg.get("c1"); !ok || ac.browserOpus.(*browserTestCodec).closed.Load() {
		t.Fatal("timeout removeu/fechou chamada já aceita")
	}
	if _, ok := s.mgr.broker.getCall("c1"); !ok || ac.cm.CurrentCall().IsEnded() {
		t.Fatal("timeout publicou encerramento de chamada aceita")
	}
}

func TestExpireRingingCallRacesAcceptance(t *testing.T) {
	for range 50 {
		s, ac := ringingTimeoutSession()
		var accepted error
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() { <-start; accepted = ac.cm.AcceptCall(context.Background(), "c1") })
		wg.Go(func() { <-start; s.expireRingingCall("c1") })
		close(start)
		wg.Wait()
		_, registered := s.reg.get("c1")
		closed := ac.browserOpus.(*browserTestCodec).closed.Load()
		if accepted == nil {
			if !registered || closed || ac.cm.CurrentCall().IsEnded() {
				t.Fatal("timeout encerrou o aceite vencedor")
			}
		} else if registered || !closed || ac.cm.CurrentCall().StateData.EndReason != core.EndCallReasonTimeout {
			t.Fatal("timeout vencedor não liberou a chamada")
		}
		ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
		s.removeCall("c1")
	}
}
