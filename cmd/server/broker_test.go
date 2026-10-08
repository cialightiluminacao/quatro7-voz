package main

import (
	"reflect"
	"testing"
)

func ownerPtr(s string) *string { return &s }

func TestOwnerActiveCall(t *testing.T) {
	b := NewBroker()
	b.upsertCall(CallRecord{SessionID: "s1", CallID: "c1", Owner: ownerPtr("op-A"), Status: StatusConnected})
	b.upsertCall(CallRecord{SessionID: "s1", CallID: "c2", Owner: ownerPtr("op-B"), Status: StatusRinging})

	if got := b.ownerActiveCall("s1", "op-A"); got != "c1" {
		t.Fatalf("op-A should own c1, got %q", got)
	}
	if got := b.ownerActiveCall("s1", "op-C"); got != "" {
		t.Fatalf("op-C owns nothing, got %q", got)
	}
	if got := b.ownerActiveCall("s1", ""); got != "" {
		t.Fatalf("empty owner must return empty, got %q", got)
	}

	b.endCall("s1", "c1", "done")
	if got := b.ownerActiveCall("s1", "op-A"); got != "" {
		t.Fatalf("op-A's call ended, expected empty, got %q", got)
	}
}

func TestReassignOwner(t *testing.T) {
	b := NewBroker()
	b.upsertCall(CallRecord{SessionID: "s1", CallID: "c1", Owner: ownerPtr("op-A"), Status: StatusConnected})

	// libera o dono (transferência oferecida)
	if !b.reassignOwner("s1", "c1", nil) {
		t.Fatal("reassignOwner deveria achar c1")
	}
	if c, ok := b.getCall("s1", "c1"); !ok || c.Owner != nil {
		t.Fatalf("esperava owner nil após liberar, got %+v", c)
	}

	// fixa novo dono (pickup)
	b.reassignOwner("s1", "c1", ownerPtr("op-B"))
	if c, ok := b.getCall("s1", "c1"); !ok || c.Owner == nil || *c.Owner != "op-B" {
		t.Fatalf("esperava owner op-B após pickup, got %+v", c)
	}
	if got := b.ownerActiveCall("s1", "op-B"); got != "c1" {
		t.Fatalf("op-B deveria ser dono de c1, got %q", got)
	}

	// chamada inexistente
	if b.reassignOwner("s1", "nao-existe", ownerPtr("x")) {
		t.Fatal("reassignOwner de callID inexistente deveria falhar")
	}
}

func TestBrokerSameCallIDCoexistsAndEndsWithinSession(t *testing.T) {
	b := NewBroker()
	a := CallRecord{SessionID: "s1", CallID: "same", Peer: "peer-A", Owner: ownerPtr("A"), StartedAt: 10, Status: StatusConnected}
	other := CallRecord{SessionID: "s2", CallID: "same", Peer: "peer-B", Owner: ownerPtr("B"), StartedAt: 20, Status: StatusRinging}
	b.upsertCall(a)
	b.upsertCall(other)
	for _, want := range []CallRecord{a, other} {
		if got, ok := b.getCall(want.SessionID, want.CallID); !ok || !reflect.DeepEqual(*got, want) {
			t.Fatalf("session %s: got %+v, want %+v", want.SessionID, got, want)
		}
	}
	sub := b.subscribe("bench", 0)
	defer b.unsubscribe(sub)
	b.endCall("s2", "same", "accepted_elsewhere")
	b.endCall("s2", "same", "duplicate") // duplicate terminal callback must be harmless
	if got, ok := b.getCall("s1", "same"); !ok || !reflect.DeepEqual(*got, a) {
		t.Fatalf("ending B changed A: %+v", got)
	}
	if _, ok := b.getCall("s2", "same"); ok {
		t.Fatal("B remains active")
	}
	ended, lists := 0, 0
	for _, ev := range drain(sub) {
		switch ev["type"] {
		case "call-ended":
			ended++
			if ev["id"] != "same" || ev["sessionId"] != "s2" || ev["reason"] != "accepted_elsewhere" || ev["owner"] != "B" {
				t.Fatalf("wrong ended event: %+v", ev)
			}
		case "call-list":
			lists++
			calls := ev["calls"].([]any)
			if len(calls) != 1 || calls[0].(map[string]any)["sessionId"] != "s1" || calls[0].(map[string]any)["callId"] != "same" {
				t.Fatalf("wrong remaining call-list: %+v", ev)
			}
		}
	}
	if ended != 1 || lists != 1 {
		t.Fatalf("ended=%d lists=%d, want one of each", ended, lists)
	}
	if rows := b.historyRows("s1", 50); len(rows) != 0 {
		t.Fatalf("A got B's history: %+v", rows)
	}
	if rows := b.historyRows("s2", 50); len(rows) != 1 || rows[0].SessionID != "s2" || rows[0].EndReason != "accepted_elsewhere" {
		t.Fatalf("B history: %+v", rows)
	}
	b.endCall("s1", "same", "done")
	if rows := b.historyRows("", 50); len(rows) != 2 || rows[0].SessionID != "s1" || rows[1].SessionID != "s2" {
		t.Fatalf("history collapsed same-ID legs: %+v", rows)
	}
}

func TestBrokerOwnersAreScopedToSession(t *testing.T) {
	b := NewBroker()
	for _, sid := range []string{"s1", "s2"} {
		b.upsertCall(CallRecord{SessionID: sid, CallID: "same", Status: StatusConnected})
	}
	if !b.setOwner("s1", "same", "A") || !b.setOwner("s2", "same", "B") {
		t.Fatal("owners on independent legs conflicted")
	}
	if got := b.ownerActiveCall("s2", "A"); got != "" {
		t.Fatalf("owner lookup crossed sessions: %q", got)
	}
	if !b.reassignOwner("s1", "same", nil) || !b.claimTransferred("s1", "same", "C") || b.claimTransferred("s1", "same", "D") {
		t.Fatal("transfer claim did not preserve ownership rules")
	}
	if got, ok := b.getCall("s2", "same"); !ok || got.Owner == nil || *got.Owner != "B" {
		t.Fatalf("claim changed B: %+v", got)
	}
	for _, sid := range []string{"", "missing"} {
		if _, ok := b.getCall(sid, "same"); ok || b.setOwner(sid, "same", "X") || b.reassignOwner(sid, "same", nil) || b.claimTransferred(sid, "same", "X") || b.ownerActiveCall(sid, "B") != "" {
			t.Fatalf("lookup without the matching session succeeded: %q", sid)
		}
		b.endCall(sid, "same", "wrong-session")
	}
	if len(b.calls) != 2 || len(b.historyRows("", 50)) != 0 {
		t.Fatal("unknown session changed a call")
	}
}

func TestBrokerCallKeyHasNoSeparatorAmbiguity(t *testing.T) {
	b := NewBroker()
	b.upsertCall(CallRecord{SessionID: "a:b", CallID: "c", Peer: "A"})
	b.upsertCall(CallRecord{SessionID: "a", CallID: "b:c", Peer: "B"})
	if len(b.calls) != 2 {
		t.Fatal("distinct session/call pairs aliased")
	}
}
