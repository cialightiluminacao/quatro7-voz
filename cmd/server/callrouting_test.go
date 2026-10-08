package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/wanode"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func TestCallIDFromNode(t *testing.T) {
	node := &waBinary.Node{
		Tag: "call",
		Content: []waBinary.Node{{
			Tag:   "offer",
			Attrs: waBinary.Attrs{"call-id": "ABC123"},
		}},
	}
	if got := callIDFromNode(node); got != "ABC123" {
		t.Fatalf("expected ABC123, got %q", got)
	}

	empty := &waBinary.Node{Tag: "call"}
	if got := callIDFromNode(empty); got != "" {
		t.Fatalf("node with no children must yield empty, got %q", got)
	}
	node.Content = append([]waBinary.Node{{Tag: "relaylatency"}}, node.GetChildren()...)
	if got := callIDFromNode(node); got != "ABC123" {
		t.Fatalf("call-id after auxiliary child = %q, want ABC123", got)
	}
}

type routingTestSocket struct {
	core.VoipSocket
	mu   sync.Mutex
	sent []waBinary.Node
}

func (*routingTestSocket) OwnLID() types.JID                                           { return wanode.MustJID("999:9@lid") }
func (*routingTestSocket) OwnPN() types.JID                                            { return wanode.MustJID("999:9@s.whatsapp.net") }
func (*routingTestSocket) ResolveLIDForPN(_ context.Context, peer types.JID) types.JID { return peer }
func (*routingTestSocket) GetUSyncDevices(context.Context, []types.JID) ([]types.JID, error) {
	return []types.JID{wanode.MustJID("123@lid"), wanode.MustJID("123:33@lid")}, nil
}
func (*routingTestSocket) AssertSessions(context.Context, []types.JID, bool) error { return nil }
func (*routingTestSocket) CreateParticipantNodes(context.Context, []types.JID, []byte, waBinary.Attrs) ([]waBinary.Node, bool, error) {
	return []waBinary.Node{{Tag: "enc", Attrs: waBinary.Attrs{"type": "msg"}}}, false, nil
}
func (*routingTestSocket) GetTCToken(context.Context, types.JID) ([]byte, error) { return nil, nil }
func (*routingTestSocket) DecryptCallKey(context.Context, types.JID, *waBinary.Node) ([]byte, error) {
	return nil, nil
}
func (*routingTestSocket) Query(context.Context, waBinary.Node) (*waBinary.Node, error) {
	return &waBinary.Node{Tag: "ack"}, nil
}
func (sock *routingTestSocket) SendNode(_ context.Context, node waBinary.Node) error {
	sock.mu.Lock()
	defer sock.mu.Unlock()
	sock.sent = append(sock.sent, node)
	return nil
}

func outgoingRoutingSession(t *testing.T) (*Session, *routingTestSocket) {
	t.Helper()
	sock := &routingTestSocket{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cm := call.NewCallManager(sock, log)
	s := &Session{reg: newCallRegistry(), log: log}
	s.reg.add("SAME_CALL_ID", &activeCall{cm: cm})
	if err := cm.StartCall(context.Background(), "SAME_CALL_ID", wanode.MustJID("123@lid"), false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.EndCall(context.Background(), core.EndCallReasonUserEnded) })
	return s, sock
}

func TestCallAcceptRoutingIsScopedToSessionWithSameCallID(t *testing.T) {
	a, _ := outgoingRoutingSession(t)
	b, _ := outgoingRoutingSession(t)
	from := wanode.MustJID("123:33@lid")
	a.handleEvent(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "SAME_CALL_ID", From: from},
		Data:          &waBinary.Node{Tag: "accept", Attrs: waBinary.Attrs{"call-id": "SAME_CALL_ID"}},
	})
	ac, _ := a.reg.get("SAME_CALL_ID")
	bc, _ := b.reg.get("SAME_CALL_ID")
	if ac.cm.CurrentCall().StateData.State != core.CallStateConnecting ||
		bc.cm.CurrentCall().StateData.State != core.CallStateRinging {
		t.Fatalf("event must affect only its session: A=%s B=%s",
			ac.cm.CurrentCall().StateData.State, bc.cm.CurrentCall().StateData.State)
	}
}

func TestUnknownCallAcceptAfterAuxiliaryChildReachesOnlyItsSession(t *testing.T) {
	for _, from := range []types.JID{wanode.MustJID("123@lid"), wanode.MustJID("123:33@lid")} {
		t.Run(from.String(), func(t *testing.T) {
			a, sock := outgoingRoutingSession(t)
			b, _ := outgoingRoutingSession(t)
			node := &waBinary.Node{
				Tag: "call", Attrs: waBinary.Attrs{"from": from, "id": "ACCEPT_STANZA"},
				Content: []waBinary.Node{
					{Tag: "relaylatency"},
					{Tag: "accept", Attrs: waBinary.Attrs{"call-id": "SAME_CALL_ID", "call-creator": "999:9@lid"}},
				},
			}
			a.handleEvent(&events.UnknownCallEvent{Node: node})
			ac, _ := a.reg.get("SAME_CALL_ID")
			bc, _ := b.reg.get("SAME_CALL_ID")
			if ac.cm.CurrentCall().StateData.State != core.CallStateConnecting ||
				bc.cm.CurrentCall().StateData.State != core.CallStateRinging {
				t.Fatalf("multi-child accept routing: A=%s B=%s",
					ac.cm.CurrentCall().StateData.State, bc.cm.CurrentCall().StateData.State)
			}
			receipt := false
			for _, sent := range sock.sent {
				if sent.Tag == "receipt" && wanode.AttrString(sent.Attrs, "id") == "ACCEPT_STANZA" &&
					wanode.AttrString(sent.Attrs, "to") == from.String() {
					receipt = true
				}
			}
			if !receipt || len(node.GetChildren()) != 2 {
				t.Fatal("raw accept must preserve id/from without modifying the original node")
			}
		})
	}
}

func TestUnknownAcceptedElsewherePreservesReasonAfterAuxiliaryChild(t *testing.T) {
	a, _ := outgoingRoutingSession(t)
	ac, _ := a.reg.get("SAME_CALL_ID")
	sock := &routingTestSocket{}
	s := &Session{reg: newCallRegistry(), log: a.log}
	// Leg B incoming, com o mesmo call-id usado pelo caller A em outro registro.
	cm := call.NewCallManager(sock, s.log)
	cm.HandleCallOffer(context.Background(), &waBinary.Node{Tag: "call", Content: []waBinary.Node{
		{Tag: "offer", Attrs: waBinary.Attrs{"call-id": "SAME_CALL_ID", "call-creator": "123@lid"}},
	}}, wanode.MustJID("123@lid"))
	t.Cleanup(func() { _ = cm.EndCall(context.Background(), core.EndCallReasonUserEnded) })
	if err := cm.AcceptCall(context.Background(), "SAME_CALL_ID"); err != nil {
		t.Fatal(err)
	}
	if err := cm.CurrentCall().ApplyTransition(call.Transition{Type: call.TransitionMediaConnected}); err != nil {
		t.Fatal(err)
	}
	s.reg.add("SAME_CALL_ID", &activeCall{cm: cm})
	s.handleEvent(&events.UnknownCallEvent{Node: &waBinary.Node{
		Tag: "call", Attrs: waBinary.Attrs{"from": wanode.MustJID("123@lid")},
		Content: []waBinary.Node{
			{Tag: "relaylatency"},
			{Tag: "terminate", Attrs: waBinary.Attrs{"call-id": "SAME_CALL_ID", "reason": "accepted_elsewhere"}},
		},
	}})
	if !cm.CurrentCall().IsActive() || ac.cm.CurrentCall().StateData.State != core.CallStateRinging {
		t.Fatal("multi-child accepted_elsewhere must preserve the active local leg")
	}
}
