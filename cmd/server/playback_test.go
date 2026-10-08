package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"wacalls/internal/voip/core"
)

func serverWAV() []byte {
	b := make([]byte, 44+3200)
	copy(b, "RIFF")
	binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-8))
	copy(b[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(b[16:], 16)
	binary.LittleEndian.PutUint16(b[20:], 1)
	binary.LittleEndian.PutUint16(b[22:], 1)
	binary.LittleEndian.PutUint32(b[24:], 16000)
	binary.LittleEndian.PutUint32(b[28:], 32000)
	binary.LittleEndian.PutUint16(b[32:], 2)
	binary.LittleEndian.PutUint16(b[34:], 16)
	copy(b[36:], "data")
	binary.LittleEndian.PutUint32(b[40:], uint32(len(b)-44))
	return b
}

func playbackHTTP(t *testing.T) (*Session, *activeCall, *http.ServeMux) {
	t.Helper()
	sess, ac := ringingTimeoutSession()
	sess.id = "s1"
	sess.mgr.sessions = map[string]*Session{"s1": sess}
	sess.wireCall(ac.cm, "c1")
	srv := &server{sessions: sess.mgr, broker: sess.mgr.broker}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/announce", srv.handleAnnounce)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/accept-announce", srv.handleAnnounce)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/hold-music", srv.handleHoldMusic)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/transfer", srv.handleTransfer)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/pickup", srv.handlePickup)
	t.Cleanup(func() { ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded); sess.removeCall("c1") })
	return sess, ac, mux
}

func playbackRequest(mux *http.ServeMux, action, owner, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/api/sessions/s1/calls/c1/"+action, strings.NewReader(body))
	r.Header.Set("X-Client-Id", owner)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestHTTPAnnounceWithoutBrowserAndAcceptAlias(t *testing.T) {
	for _, action := range []string{"announce", "accept-announce"} {
		t.Run(action, func(t *testing.T) {
			_, ac, mux := playbackHTTP(t)
			body := `{"audioBase64":"` + base64.StdEncoding.EncodeToString(serverWAV()) + `","hangupAfter":true,"maxSeconds":2}`
			w := playbackRequest(mux, action, "", body)
			if w.Code != http.StatusAccepted || ac.cm.VideoState().State != core.CallStateConnecting || !ac.answered.Load() || ac.bridge != nil || ac.wsBridge != nil {
				t.Fatalf("status=%d body=%s state=%+v", w.Code, w.Body.String(), ac.cm.VideoState())
			}
			var response map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response["status"] != "playing" || response["maxSeconds"] != float64(2) || response["hangupAfter"] != true {
				t.Fatal("incorrect playback contract")
			}
			if second := playbackRequest(mux, action, "", body); second.Code != http.StatusConflict {
				t.Fatal("concurrent playback accepted")
			}
		})
	}
}

func TestHTTPPlaybackAudioValidation(t *testing.T) {
	for _, tc := range []struct {
		body string
		code int
	}{
		{`{"audioBase64":"broken"}`, 400},
		{`{"audioBase64":"` + base64.StdEncoding.EncodeToString([]byte("not WAV")) + `"}`, 400},
		{`{}`, 400},
		{`{"audioBase64":"eA==","maxSeconds":0}`, 400},
		{`{"audioBase64":"eA==","maxSeconds":301}`, 400},
		{`{"audioBase64":"eA==","maxSeconds":1.5}`, 400},
		{`{"unknown":true}`, 400},
		{`{} {}`, 400},
	} {
		t.Run(tc.body[:min(50, len(tc.body))], func(t *testing.T) {
			_, _, mux := playbackHTTP(t)
			if w := playbackRequest(mux, "announce", "", tc.body); w.Code != tc.code {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	// MaxBytesReader applies before decoding even a syntactically valid huge string.
	r := httptest.NewRequest("POST", "/", bytes.NewReader(bytes.Repeat([]byte{' '}, maxPlaybackJSONBytes+1)))
	if readPlaybackJSON(httptest.NewRecorder(), r, new(playbackAudio)) == nil {
		t.Fatal("oversized JSON accepted")
	}
}

func TestHTTPHoldMusicDetachesThenTransferPickupAttach(t *testing.T) {
	for _, transport := range []string{"webrtc", "ws"} {
		t.Run(transport, func(t *testing.T) {
			sess, ac, mux := playbackHTTP(t)
			ac.cm.CurrentCall().StateData.State = core.CallStateActive
			a := &Bridge{}
			sess.setBridge("c1", a, &browserTestCodec{})
			ac.playoutMu.Lock()
			ac.playout.push(make([]float32, 2880))
			ac.playoutMu.Unlock()
			before := ac.cm.VideoState()
			w := playbackRequest(mux, "hold-music", "A", `{"audioBase64":"`+base64.StdEncoding.EncodeToString(serverWAV())+`"}`)
			if w.Code != 200 || ac.bridge != nil || ac.wsBridge != nil || !a.terminateDisabled.Load() || ac.cm.VideoState().State != core.CallStateOnHold {
				t.Fatalf("hold: status=%d body=%s", w.Code, w.Body.String())
			}
			w = playbackRequest(mux, "transfer", "A", `{"to":" B "}`)
			if w.Code != 200 {
				t.Fatalf("transfer: status=%d body=%s", w.Code, w.Body.String())
			}
			if w := playbackRequest(mux, "pickup", "C", ""); w.Code != 409 {
				t.Fatal("targeted transfer captured by C")
			}
			w = playbackRequest(mux, "pickup", "B", "")
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"callId":"c1"`) {
				t.Fatalf("pickup: status=%d body=%s", w.Code, w.Body.String())
			}
			if transport == "webrtc" {
				sess.setBridge("c1", &Bridge{}, &browserTestCodec{})
			} else {
				sess.setWSBridge("c1", &wsBridge{closed: true, cancel: func() {}}, &browserTestCodec{})
			}
			after := ac.cm.VideoState()
			if after.State != core.CallStateActive || after.PeerOrientation != before.PeerOrientation || after.PeerVideoOn != before.PeerVideoOn || after.VideoOff != before.VideoOff {
				t.Fatalf("swap lost video snapshot or failed to resume: %+v", after)
			}
			ac.playoutMu.Lock()
			level := ac.playout.level()
			ac.playoutMu.Unlock()
			if level != 0 {
				t.Fatal("old browser playout survived swap")
			}
		})
	}
}

func TestPickupHasOneWinnerAndBridgeTransportSwap(t *testing.T) {
	sess, ac, mux := playbackHTTP(t)
	ac.cm.CurrentCall().StateData.State = core.CallStateActive
	if w := playbackRequest(mux, "transfer", "A", "{}"); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for _, owner := range []string{"B", "C"} {
		wg.Go(func() { <-start; codes <- playbackRequest(mux, "pickup", owner, "").Code })
	}
	close(start)
	wg.Wait()
	c1, c2 := <-codes, <-codes
	if c1+c2 != 200+409 {
		t.Fatalf("simultaneous pickups: %d %d", c1, c2)
	}
	oldCodec := &browserTestCodec{}
	oldWS := &wsBridge{closed: true, cancel: func() {}}
	sess.setWSBridge("c1", oldWS, oldCodec)
	epoch := ac.mediaEpoch.Load()
	bridge := &Bridge{}
	sess.setBridge("c1", bridge, &browserTestCodec{})
	if ac.wsBridge != nil || !oldWS.terminateDisabled.Load() || !oldCodec.closed.Load() || bridge.mediaEpoch.Load() <= epoch {
		t.Fatal("WebRTC swap left old WebSocket bridge or epoch active")
	}
}

func TestHTTPTransferMissingBrokerCallPreservesBrowser(t *testing.T) {
	sess, ac, mux := playbackHTTP(t)
	ac.cm.CurrentCall().StateData.State = core.CallStateActive
	bridge := &Bridge{}
	sess.setBridge("c1", bridge, &browserTestCodec{})
	sess.mgr.broker.mu.Lock()
	delete(sess.mgr.broker.calls, "c1")
	sess.mgr.broker.mu.Unlock()
	w := playbackRequest(mux, "transfer", "A", `{"to":"B"}`)
	if w.Code != 404 || ac.bridge != bridge || bridge.terminateDisabled.Load() || ac.cm.VideoState().State != core.CallStateActive || ac.transferTarget != "" {
		t.Fatalf("missing broker call changed media: status=%d", w.Code)
	}
}

func TestHTTPAnnounceRejectsHumanOwner(t *testing.T) {
	for _, action := range []string{"announce", "accept-announce"} {
		t.Run(action, func(t *testing.T) {
			sess, ac, mux := playbackHTTP(t)
			if !sess.mgr.broker.setOwner("c1", "A") {
				t.Fatal("missing broker call")
			}
			w := playbackRequest(mux, action, "", `{"audioBase64":"`+base64.StdEncoding.EncodeToString(serverWAV())+`"}`)
			if w.Code != 409 || ac.answered.Load() || ac.cm.VideoState().State != core.CallStateIncomingRinging {
				t.Fatalf("owned call announced: status=%d state=%+v", w.Code, ac.cm.VideoState())
			}
		})
	}
}

func TestHTTPHoldMusicRejectsActiveAnnounce(t *testing.T) {
	_, ac, mux := playbackHTTP(t)
	body := `{"audioBase64":"` + base64.StdEncoding.EncodeToString(serverWAV()) + `","hangupAfter":true}`
	if w := playbackRequest(mux, "announce", "", body); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	ac.cm.CurrentCall().StateData.State = core.CallStateActive
	w := playbackRequest(mux, "hold-music", "", `{"audioBase64":"`+base64.StdEncoding.EncodeToString(serverWAV())+`"}`)
	if w.Code != 409 || ac.cm.VideoState().State != core.CallStateActive {
		t.Fatalf("announce replaced: status=%d", w.Code)
	}
}
