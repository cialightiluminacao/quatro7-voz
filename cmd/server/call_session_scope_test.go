package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"wacalls/internal/voip/core"

	waBinary "go.mau.fi/whatsmeow/binary"
)

type sameIDCalls struct {
	srv      *server
	a, b     *Session
	acA, acB *activeCall
	handler  http.Handler
}

func newSameIDCalls(t *testing.T) sameIDCalls {
	t.Helper()
	t.Setenv("WACALLS_API_KEY", "")
	a, acA := ringingTimeoutSession()
	b, acB := ringingTimeoutSession()
	b.id = "s2"
	b.mgr = a.mgr
	a.mgr.sessions = map[string]*Session{a.id: a, b.id: b}
	for _, sess := range []*Session{a, b} {
		ac, _ := sess.reg.get("c1")
		sess.wireCall(ac.cm, "c1")
	}
	a.mgr.broker.upsertCall(CallRecord{SessionID: a.id, CallID: "c1", Peer: "peer-A", Direction: "inbound", StartedAt: 10, Status: StatusRinging})
	a.mgr.broker.upsertCall(CallRecord{SessionID: b.id, CallID: "c1", Peer: "peer-B", Direction: "inbound", StartedAt: 20, Status: StatusRinging, Owner: ownerPtr("B")})
	srv := &server{sessions: a.mgr, broker: a.mgr.broker, log: a.log}
	t.Cleanup(func() {
		for _, sess := range []*Session{a, b} {
			if ac, ok := sess.reg.get("c1"); ok {
				_ = ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
			}
			sess.removeCall("c1")
		}
	})
	return sameIDCalls{srv: srv, a: a, b: b, acA: acA, acB: acB, handler: srv.routes()}
}

func scopeRequest(handler http.Handler, method, path, owner, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("X-Client-Id", owner)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestHTTPCallActionsWithSameIDAreScopedToSession(t *testing.T) {
	wavBody := `{"audioBase64":"` + base64.StdEncoding.EncodeToString(serverWAV()) + `"}`
	for _, tc := range []struct {
		action string
		state  core.CallState
		body   string
		code   int
	}{
		{"hangup", core.CallStateActive, "", http.StatusNoContent},
		{"accept", core.CallStateIncomingRinging, "", http.StatusOK},
		{"reject", core.CallStateIncomingRinging, "", http.StatusOK},
		{"transfer", core.CallStateActive, `{"to":"new-A"}`, http.StatusOK},
		{"pickup", core.CallStateOnHold, "", http.StatusOK},
		{"announce", core.CallStateIncomingRinging, wavBody, http.StatusAccepted},
		{"accept-announce", core.CallStateIncomingRinging, wavBody, http.StatusAccepted},
		{"hold-music", core.CallStateActive, wavBody, http.StatusOK},
		{"hold", core.CallStateActive, "{}", http.StatusOK},
		{"resume", core.CallStateOnHold, "", http.StatusOK},
	} {
		t.Run(tc.action, func(t *testing.T) {
			f := newSameIDCalls(t)
			f.acA.cm.CurrentCall().StateData.State = tc.state
			// B has its own browser leg; no A operation may close or replace it.
			bridgeB, codecB := &Bridge{}, &browserTestCodec{}
			f.b.setBridge("c1", bridgeB, codecB)
			beforeB, _ := f.srv.broker.getCall(f.b.id, "c1")
			stateB := f.acB.cm.VideoState()
			epochB := f.acB.mediaEpoch.Load()
			if tc.action == "transfer" {
				f.a.setBridge("c1", &Bridge{}, &browserTestCodec{})
			}
			method, path := http.MethodPost, "/api/sessions/s1/calls/c1/"+tc.action
			if tc.action == "hangup" {
				method, path = http.MethodDelete, "/api/sessions/s1/calls/c1"
			}
			w := scopeRequest(f.handler, method, path, "A", tc.body)
			if w.Code != tc.code {
				t.Fatalf("status=%d body=%s, want %d", w.Code, w.Body.String(), tc.code)
			}
			afterB, ok := f.srv.broker.getCall(f.b.id, "c1")
			if !ok || !reflect.DeepEqual(beforeB, afterB) || !reflect.DeepEqual(stateB, f.acB.cm.VideoState()) {
				t.Fatalf("A changed B: before=%+v after=%+v state=%+v", beforeB, afterB, f.acB.cm.VideoState())
			}
			if ac, ok := f.b.reg.get("c1"); !ok || ac != f.acB || ac.bridge != bridgeB || ac.browserOpus != codecB || codecB.closed.Load() || bridgeB.terminateDisabled.Load() || ac.mediaEpoch.Load() != epochB {
				t.Fatal("A changed B's browser registry or transport")
			}
			recA, active := f.srv.broker.getCall(f.a.id, "c1")
			switch tc.action {
			case "hangup", "reject":
				if active {
					t.Fatal("A was not ended")
				}
			case "accept", "pickup":
				if !active || recA.Owner == nil || *recA.Owner != "A" || recA.StartedAt != 10 {
					t.Fatalf("A claim used the wrong record: %+v", recA)
				}
			case "transfer":
				if !active || !recA.Held || recA.Owner != nil || f.acA.transferTarget != "new-A" || f.acA.bridge != nil {
					t.Fatalf("A was not transferred: %+v", recA)
				}
			}
		})
	}
}

func TestHTTPCallRoutesNeverFallBackToAnotherSession(t *testing.T) {
	f := newSameIDCalls(t)
	_ = f.acA.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
	beforeB, _ := f.srv.broker.getCall(f.b.id, "c1")
	for _, tc := range []struct{ method, suffix string }{
		{http.MethodDelete, ""},
		{http.MethodPost, "/accept"}, {http.MethodPost, "/reject"},
		{http.MethodPost, "/transfer"}, {http.MethodPost, "/pickup"},
		{http.MethodPost, "/announce"}, {http.MethodPost, "/accept-announce"},
		{http.MethodPost, "/hold-music"}, {http.MethodPost, "/hold"},
		{http.MethodPost, "/resume"}, {http.MethodPost, "/webrtc"},
		{http.MethodPost, "/video/request"}, {http.MethodGet, "/ws"},
	} {
		w := scopeRequest(f.handler, tc.method, "/api/sessions/s1/calls/c1"+tc.suffix, "A", "{}")
		want := http.StatusNotFound
		if tc.method == http.MethodDelete {
			want = http.StatusNoContent // preserve idempotent hangup response
		}
		if w.Code != want {
			t.Fatalf("%s %s: status=%d body=%s", tc.method, tc.suffix, w.Code, w.Body.String())
		}
	}
	// No legacy call-ID-only route is registered. It must never scan sessions.
	for _, tc := range []struct{ method, path string }{
		{http.MethodDelete, "/api/calls/c1"},
		{http.MethodPost, "/api/calls/c1/accept"},
		{http.MethodPost, "/api/calls/c1/transfer"},
	} {
		if w := scopeRequest(f.handler, tc.method, tc.path, "A", "{}"); w.Code != http.StatusNotFound {
			t.Fatalf("ID-only route resolved a call: %s %s status=%d", tc.method, tc.path, w.Code)
		}
	}
	afterB, ok := f.srv.broker.getCall(f.b.id, "c1")
	if !ok || !reflect.DeepEqual(beforeB, afterB) || f.acB.cm.CurrentCall().IsEnded() {
		t.Fatalf("missing A affected B: %+v", afterB)
	}
}

func TestAcceptedElsewhereCallbackKeepsOtherSessionTransferable(t *testing.T) {
	f := newSameIDCalls(t)
	f.acA.cm.CurrentCall().Direction = core.CallDirectionOutgoing
	f.acA.cm.CurrentCall().StateData.State = core.CallStateActive
	f.srv.broker.upsertCall(CallRecord{SessionID: f.a.id, CallID: "c1", Direction: "outbound", Peer: "peer-A", Status: StatusConnected, StartedAt: 10, Owner: ownerPtr("A")})
	sub := f.srv.broker.subscribe("bench", 0)
	defer f.srv.broker.unsubscribe(sub)
	f.acB.cm.HandleCallTerminate(&waBinary.Node{Tag: "call", Content: []waBinary.Node{
		{Tag: "terminate", Attrs: waBinary.Attrs{"call-id": "c1", "reason": "accepted_elsewhere"}},
	}})
	ended := 0
	for _, ev := range drain(sub) {
		if ev["type"] == "call-ended" {
			ended++
			if ev["sessionId"] != f.b.id || ev["id"] != "c1" || ev["reason"] != "accepted_elsewhere" {
				t.Fatalf("spurious end event: %+v", ev)
			}
		}
		if ev["type"] == "call-list" {
			calls := ev["calls"].([]any)
			if len(calls) != 1 || calls[0].(map[string]any)["sessionId"] != f.a.id {
				t.Fatalf("outbound A vanished from call-list: %+v", ev)
			}
		}
	}
	if ended != 1 || f.acA.cm.CurrentCall().IsEnded() {
		t.Fatalf("ended=%d, A state=%+v", ended, f.acA.cm.VideoState())
	}
	w := scopeRequest(f.handler, http.MethodPost, "/api/sessions/s1/calls/c1/transfer", "A", `{"to":"new-A"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("A transfer failed after B ended: status=%d body=%s", w.Code, w.Body.String())
	}
}
