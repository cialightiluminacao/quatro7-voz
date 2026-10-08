package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"wacalls/internal/voip/core"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestHTTPVideoResponseMatchesSSE(t *testing.T) {
	for _, tc := range []struct {
		action, body, motivo string
		peerVideo, pending   bool
	}{
		{action: "request", motivo: "upgrade-requested"},
		{action: "request", peerVideo: true, motivo: "local-enabled"},
		{action: "accept", motivo: "upgrade-accepted"},
		{action: "accept", body: `{"withCamera":false}`, motivo: "upgrade-accepted"},
		{action: "reject", motivo: "upgrade-rejected"},
		{action: "stop", motivo: "local-stopped"},
		{action: "stop", pending: true, motivo: "upgrade-cancelled"},
	} {
		t.Run(tc.action+tc.motivo+tc.body, func(t *testing.T) {
			sess, ac := ringingTimeoutSession()
			sess.id = "s1"
			c := ac.cm.CurrentCall()
			c.StateData.State = core.CallStateActive
			c.StateData.PeerVideoOn, c.StateData.VideoUpgradeOutgoing = tc.peerVideo, tc.pending
			c.StateData.PeerOrientation = 2
			sess.wireCall(ac.cm, "c1")
			t.Cleanup(func() { ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded) })
			sess.mgr.sessions = map[string]*Session{"s1": sess}
			srv := &server{sessions: sess.mgr, broker: sess.mgr.broker}
			sub := srv.broker.subscribe("test", 0)
			drain(sub)
			mux := http.NewServeMux()
			mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/video/{action}", srv.handleCallVideo)
			r := httptest.NewRequest("POST", "/api/sessions/s1/calls/c1/video/"+tc.action, strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			var response struct {
				OK         bool           `json:"ok"`
				VideoState map[string]any `json:"videoState"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			var event map[string]any
			for _, e := range drain(sub) {
				if e["type"] == "video-state" {
					event = e
				}
			}
			if !response.OK || !reflect.DeepEqual(response.VideoState, event) || len(event) != 10 || event["motivo"] != tc.motivo || event["peerOrientation"] != float64(2) {
				t.Fatalf("resposta=%s evento=%v", w.Body.String(), event)
			}
		})
	}
}

type httpVideoGateSocket struct {
	timeoutTestSocket
	entered, release chan struct{}
	once             sync.Once
}

func (s *httpVideoGateSocket) SendNode(ctx context.Context, n waBinary.Node) error {
	for _, child := range n.GetChildren() {
		if child.Tag == "video" {
			s.once.Do(func() { close(s.entered); <-s.release })
		}
	}
	return s.timeoutTestSocket.SendNode(ctx, n)
}

func TestHTTPVideoResponseKeepsActionSnapshotDuringPeerUpdate(t *testing.T) {
	for _, action := range []string{"request", "accept", "reject", "stop"} {
		t.Run(action, func(t *testing.T) {
			sock := &httpVideoGateSocket{entered: make(chan struct{}), release: make(chan struct{})}
			sess, ac := ringingTimeoutSessionWithSocket(sock)
			sess.id = "s1"
			c := ac.cm.CurrentCall()
			c.StateData.State = core.CallStateActive
			c.StateData.VideoUpgradeIncoming = action == "accept" || action == "reject"
			sess.wireCall(ac.cm, "c1")
			defer ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
			sess.mgr.sessions = map[string]*Session{"s1": sess}
			srv := &server{sessions: sess.mgr, broker: sess.mgr.broker}
			sub := srv.broker.subscribe("test", 0)
			drain(sub)
			r := httptest.NewRequest("POST", "/", nil)
			r.SetPathValue("sid", "s1")
			r.SetPathValue("id", "c1")
			r.SetPathValue("action", action)
			w := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { srv.handleCallVideo(w, r); close(done) }()
			select {
			case <-sock.entered:
			case <-time.After(time.Second):
				close(sock.release)
				<-done
				t.Fatal("ação não chegou ao envio")
			}
			var actionEvent map[string]any
			for _, event := range drain(sub) {
				if event["type"] == "video-state" {
					actionEvent = event
				}
			}
			ac.cm.HandleVideoState(context.Background(), &waBinary.Node{
				Tag: "call", Attrs: waBinary.Attrs{"from": types.NewJID("123", types.HiddenUserServer), "id": "peer-stop"},
				Content: []waBinary.Node{{Tag: "video", Attrs: waBinary.Attrs{"call-id": "c1", "state": "6"}}},
			})
			close(sock.release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("resposta HTTP não terminou")
			}
			var response struct {
				OK         bool           `json:"ok"`
				VideoState map[string]any `json:"videoState"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusOK || !response.OK || !reflect.DeepEqual(response.VideoState, actionEvent) || ac.cm.VideoState().VideoMotivo != "peer-stopped" {
				t.Fatalf("HTTP perdeu snapshot da ação: resposta=%v SSE=%v atual=%+v", response, actionEvent, ac.cm.VideoState())
			}
		})
	}
}

func TestHTTPVideoFailureKeepsConflict(t *testing.T) {
	sess, ac := ringingTimeoutSession()
	defer ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
	sess.mgr.sessions = map[string]*Session{"s1": sess}
	srv := &server{sessions: sess.mgr}
	r := httptest.NewRequest("POST", "/", nil)
	r.SetPathValue("sid", "s1")
	r.SetPathValue("id", "c1")
	r.SetPathValue("action", "request")
	w := httptest.NewRecorder()
	srv.handleCallVideo(w, r)
	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusConflict || len(response) != 1 || response["error"] == nil {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestVideoOrientationSnapshotRange(t *testing.T) {
	for _, orientation := range []int{-1, 0, 1, 2, 3, 4} {
		// O último valor válido será preenchido pelo leitor de orientação da W3.
		sess, ac := ringingTimeoutSession()
		c := ac.cm.CurrentCall()
		c.StateData.PeerOrientation = orientation
		got := videoStatePayload(sess.id, c.CallID, "state", ac.cm.VideoState())["peerOrientation"]
		want := orientation
		if want < 0 || want > 3 {
			want = 0
		}
		if got != want {
			t.Fatalf("orientation=%d snapshot=%v", orientation, got)
		}
		ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
	}
}

func TestResumeFlushesSessionPlayoutAndKeepsCadence(t *testing.T) {
	sess, ac := ringingTimeoutSession()
	c := ac.cm.CurrentCall()
	c.StateData.State = core.CallStateActive
	sess.wireCall(ac.cm, "c1")
	defer ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
	ac.playout.push(blocoConst(1600*48, 0.5))
	ac.playout.next()
	if err := ac.cm.Hold(false); err != nil {
		t.Fatal(err)
	}
	beforeIn, beforeDrop, target := ac.playout.in, ac.playout.dropped(), ac.playout.targetMs()
	if err := ac.cm.Resume(); err != nil {
		t.Fatal(err)
	}
	if ac.playout.level() != 0 || ac.playout.in != beforeIn || ac.playout.dropped() != beforeDrop || ac.playout.targetMs() != target {
		t.Fatal("retomada não limpou PCM mantendo métricas/alvo")
	}
	for range 5 {
		if f, emit := ac.playout.next(); !emit || f != nil {
			t.Fatal("retomada deve emitir silêncio por tick até re-prime")
		}
	}
	ac.playout.push(blocoConst(target*48, 0.25))
	if f, emit := ac.playout.next(); !emit || len(f) != playoutFrame48 || f[0] != 0.25 {
		t.Fatal("retomada reproduziu PCM antigo")
	}
}
