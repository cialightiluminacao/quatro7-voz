package call

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"wacalls/internal/voip/core"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func TestPeerVideoTransitions(t *testing.T) {
	for _, tc := range []struct {
		name, state, motivo                            string
		pending, peer, incoming, local                 bool
		wantPending, wantPeer, wantIncoming, wantLocal bool
		confirm                                        bool
	}{
		{name: "disabled", state: "0", peer: true, local: true, wantLocal: true, motivo: "peer-stopped"},
		{name: "enabled", state: "1", wantPeer: true, motivo: "peer-enabled"},
		{name: "enabled accepts pending", state: "1", pending: true, wantPeer: true, wantLocal: true, confirm: true, motivo: "upgrade-accepted"},
		{name: "legacy request", state: "3", wantIncoming: true, motivo: "upgrade-requested"},
		{name: "accept pending", state: "4", pending: true, wantLocal: true, confirm: true, motivo: "upgrade-accepted"},
		{name: "unsolicited accept", state: "4"},
		{name: "reject", state: "5", pending: true, motivo: "upgrade-rejected"},
		{name: "peer stopped call continues", state: "6", peer: true, local: true, wantLocal: true, motivo: "peer-stopped"},
		{name: "peer cancels own request", state: "8", incoming: true, motivo: "upgrade-cancelled"},
		{name: "request", state: "11", wantIncoming: true, motivo: "upgrade-requested"},
		{name: "unknown", state: "99"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sock := &fakeSock{}
			m, c := activeManager(sock)
			c.StateData.VideoUpgradeOutgoing, c.StateData.VideoUpgradeIncoming = tc.pending, tc.incoming
			c.StateData.PeerVideoOn, c.StateData.VideoOff = tc.peer, !tc.local
			c.StateData.PeerOrientation = 3
			var snapshots []CallStateData
			m.OnVideoStateChanged = func(c *CallInfo) { snapshots = append(snapshots, c.StateData) }
			m.HandleVideoState(context.Background(), incomingVideoNode(tc.state))
			sd := m.VideoState()
			if sd.VideoUpgradeOutgoing != tc.wantPending || sd.PeerVideoOn != tc.wantPeer || sd.VideoUpgradeIncoming != tc.wantIncoming || !sd.VideoOff != tc.wantLocal || sd.VideoMotivo != tc.motivo || sd.PeerOrientation != 3 || sd.State != core.CallStateActive {
				t.Fatalf("snapshot incorreto: %+v", sd)
			}
			wantEvents := 1
			if tc.motivo == "" {
				wantEvents = 0
			}
			if len(snapshots) != wantEvents {
				t.Fatalf("eventos=%d, quero %d", len(snapshots), wantEvents)
			}
			if got := sock.videoStateSent(); (len(got) == 1 && got[0] == "1") != tc.confirm {
				t.Fatalf("confirmações=%v", got)
			}
			if tags := sock.sentTags(); len(tags) == 0 || tags[0] != "ack" {
				t.Fatalf("ack ausente: %v", tags)
			}
		})
	}
}

func TestLocalVideoActionsEmitSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name, motivo                   string
		video, peer, pending, incoming bool
		action                         func(*CallManager) (CallStateData, error)
		states                         []string
		local, outgoing                bool
	}{
		{name: "request audio", motivo: "upgrade-requested", action: func(m *CallManager) (CallStateData, error) { return m.RequestVideoUpgrade(context.Background()) }, states: []string{"11"}, outgoing: true},
		{name: "request video", video: true, motivo: "local-enabled", action: func(m *CallManager) (CallStateData, error) { return m.RequestVideoUpgrade(context.Background()) }, states: []string{"1"}, local: true},
		{name: "request peer video", peer: true, motivo: "local-enabled", action: func(m *CallManager) (CallStateData, error) { return m.RequestVideoUpgrade(context.Background()) }, states: []string{"1"}, local: true},
		{name: "accept camera", incoming: true, motivo: "upgrade-accepted", action: func(m *CallManager) (CallStateData, error) {
			return m.AcceptVideoUpgradeWith(context.Background(), true)
		}, states: []string{"4", "1"}, local: true},
		{name: "accept receive only", incoming: true, motivo: "upgrade-accepted", action: func(m *CallManager) (CallStateData, error) {
			return m.AcceptVideoUpgradeWith(context.Background(), false)
		}, states: []string{"4"}},
		{name: "reject", incoming: true, motivo: "upgrade-rejected", action: func(m *CallManager) (CallStateData, error) { return m.RejectVideoUpgrade(context.Background()) }, states: []string{"5"}},
		{name: "stop", video: true, motivo: "local-stopped", action: func(m *CallManager) (CallStateData, error) { return m.StopVideo(context.Background()) }, states: []string{"6"}},
		{name: "cancel", pending: true, motivo: "upgrade-cancelled", action: func(m *CallManager) (CallStateData, error) { return m.StopVideo(context.Background()) }, states: []string{"8"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sock := &fakeSock{}
			m, c := activeManager(sock)
			if tc.video {
				c.MediaType = core.CallMediaTypeVideo
			}
			c.StateData.VideoUpgradeOutgoing, c.StateData.VideoUpgradeIncoming = tc.pending, tc.incoming
			c.StateData.PeerVideoOn = tc.peer
			c.StateData.PeerOrientation = 2
			var snapshots []CallStateData
			m.OnVideoStateChanged = func(c *CallInfo) { snapshots = append(snapshots, c.StateData) }
			snapshot, err := tc.action(m)
			if err != nil {
				t.Fatal(err)
			}
			sd := m.VideoState()
			if len(snapshots) != 1 || snapshots[0] != sd || snapshot != sd || sd.VideoMotivo != tc.motivo || !sd.VideoOff != tc.local || sd.VideoUpgradeOutgoing != tc.outgoing || sd.VideoUpgradeIncoming || sd.PeerOrientation != 2 {
				t.Fatalf("snapshots=%+v estado=%+v", snapshots, sd)
			}
			if got := sock.videoStateSent(); !reflect.DeepEqual(got, tc.states) {
				t.Fatalf("stanzas=%v, quero %v", got, tc.states)
			}
		})
	}
}

type gatedVideoSocket struct {
	fakeSock
	tag, state       string
	entered, release chan struct{}
	once             sync.Once
	err              error
}

func (s *gatedVideoSocket) SendNode(ctx context.Context, n waBinary.Node) error {
	match := n.Tag == s.tag
	if s.state != "" {
		match = false
		for _, c := range n.GetChildren() {
			if c.Tag == "video" && c.Attrs["state"] == s.state {
				match = true
			}
		}
	}
	if match {
		s.once.Do(func() {
			if s.entered != nil {
				close(s.entered)
				<-s.release
			}
		})
		if s.err != nil {
			return s.err
		}
	}
	return s.fakeSock.SendNode(ctx, n)
}

func TestVideoLateAcceptAfterCancel(t *testing.T) {
	m, _ := activeManager(&fakeSock{})
	if _, err := m.RequestVideoUpgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.StopVideo(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.HandleVideoState(context.Background(), incomingVideoNode("4"))
	sd := m.VideoState()
	if !sd.VideoOff || sd.VideoUpgradeOutgoing || sd.VideoMotivo != "upgrade-cancelled" {
		t.Fatalf("aceite antigo aplicado: %+v", sd)
	}
}

func TestVideoEpochIgnoresAnswerInFlightAcrossCancelAndNewRequest(t *testing.T) {
	sock := &gatedVideoSocket{tag: "ack", entered: make(chan struct{}), release: make(chan struct{})}
	m, _ := activeManager(sock)
	if _, err := m.RequestVideoUpgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { m.HandleVideoState(context.Background(), incomingVideoNode("4")); close(done) }()
	waitMediaTest(t, sock.entered)
	if _, err := m.StopVideo(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RequestVideoUpgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(sock.release)
	waitMediaTest(t, done)
	sd := m.VideoState()
	if !sd.VideoOff || !sd.VideoUpgradeOutgoing || sd.VideoMotivo != "upgrade-requested" {
		t.Fatalf("resposta do epoch anterior aplicada: %+v", sd)
	}
}

func TestVideoFailedActionRollsBackAndEmitsFailure(t *testing.T) {
	sock := &gatedVideoSocket{state: "11", err: errors.New("falha de teste")}
	m, _ := activeManager(sock)
	var reasons []string
	m.OnVideoStateChanged = func(c *CallInfo) { reasons = append(reasons, c.StateData.VideoMotivo) }
	if _, err := m.RequestVideoUpgrade(context.Background()); err == nil {
		t.Fatal("esperava erro")
	}
	sd := m.VideoState()
	if !sd.VideoOff || sd.VideoUpgradeOutgoing || !reflect.DeepEqual(reasons, []string{"upgrade-requested", "falha"}) {
		t.Fatalf("estado=%+v motivos=%v", sd, reasons)
	}
}

func TestPeerVideoAcceptanceConfirmationFailureTurnsCameraOff(t *testing.T) {
	for _, state := range []string{"4", "1"} {
		t.Run(state, func(t *testing.T) {
			sock := &gatedVideoSocket{state: "1", err: errors.New("falha de teste")}
			m, c := activeManager(sock)
			c.StateData.VideoUpgradeOutgoing = true
			var snapshots []CallStateData
			m.OnVideoStateChanged = func(c *CallInfo) { snapshots = append(snapshots, c.StateData) }
			m.HandleVideoState(context.Background(), incomingVideoNode(state))
			sd := m.VideoState()
			if !sd.VideoOff || sd.VideoMotivo != "falha" || sd.PeerVideoOn != (state == "1") || len(snapshots) != 2 || snapshots[0].VideoOff || snapshots[1] != sd {
				t.Fatalf("confirmação falhou sem desligar câmera: estado=%+v snapshots=%+v", sd, snapshots)
			}
			if got := sock.videoStateSent(); len(got) != 0 {
				t.Fatalf("envio com erro registrado como sucesso: %v", got)
			}
		})
	}
}

func TestVideoAcceptPartialFailureKeepsAcceptanceWithoutCamera(t *testing.T) {
	for _, state := range []string{"4", "1"} {
		t.Run("failure on "+state, func(t *testing.T) {
			sock := &gatedVideoSocket{state: state, err: errors.New("falha de teste")}
			m, c := activeManager(sock)
			c.StateData.VideoUpgradeIncoming = true
			var snapshots []CallStateData
			m.OnVideoStateChanged = func(c *CallInfo) { snapshots = append(snapshots, c.StateData) }
			if _, err := m.AcceptVideoUpgradeWith(context.Background(), true); err == nil {
				t.Fatal("esperava erro no envio")
			}
			sd := m.VideoState()
			if !sd.VideoOff || sd.VideoUpgradeIncoming != (state == "4") || sd.VideoMotivo != "falha" || len(snapshots) != 2 || snapshots[1] != sd {
				t.Fatalf("rollback incorreto: estado=%+v snapshots=%+v", sd, snapshots)
			}
			wantMedia := core.CallMediaTypeAudio
			var wantNodes []string
			if state == "1" {
				wantMedia, wantNodes = core.CallMediaTypeVideo, []string{"4"}
			}
			if c.MediaType != wantMedia || !reflect.DeepEqual(sock.videoStateSent(), wantNodes) {
				t.Fatalf("media=%v stanzas=%v", c.MediaType, sock.videoStateSent())
			}
		})
	}
}

func TestVideoEpochDoesNotSendEnabledAfterLocalStop(t *testing.T) {
	sock := &gatedVideoSocket{state: "4", entered: make(chan struct{}), release: make(chan struct{})}
	m, _ := activeManager(sock)
	stopped := make(chan struct{})
	m.OnVideoStateChanged = func(c *CallInfo) {
		if c.StateData.VideoMotivo == "local-stopped" {
			close(stopped)
		}
	}
	done := make(chan struct{})
	go func() { _ = m.AcceptVideoUpgrade(context.Background()); close(done) }()
	waitMediaTest(t, sock.entered)
	stopDone := make(chan struct{})
	go func() { _, _ = m.StopVideo(context.Background()); close(stopDone) }()
	waitMediaTest(t, stopped)
	close(sock.release)
	waitMediaTest(t, done)
	waitMediaTest(t, stopDone)
	for _, state := range sock.videoStateSent() {
		if state == "1" {
			t.Fatal("confirmação antiga religou câmera")
		}
	}
	if sd := m.VideoState(); !sd.VideoOff || sd.VideoMotivo != "local-stopped" {
		t.Fatalf("snapshot=%+v", sd)
	}
	if got := sock.videoStateSent(); !reflect.DeepEqual(got, []string{"4", "6"}) {
		t.Fatalf("ordem de envio=%v", got)
	}
}

func TestVideoFailedSendPreservesPeerStateAndAudioMute(t *testing.T) {
	sock := &gatedVideoSocket{state: "11", entered: make(chan struct{}), release: make(chan struct{}), err: errors.New("falha de teste")}
	m, _ := activeManager(sock)
	done := make(chan struct{})
	go func() { _, _ = m.RequestVideoUpgrade(context.Background()); close(done) }()
	waitMediaTest(t, sock.entered)
	m.notePeerVideoActive()
	m.mu.Lock()
	m.currentCall.StateData.AudioMuted = true
	m.currentCall.StateData.PeerOrientation = 3
	m.mu.Unlock()
	close(sock.release)
	waitMediaTest(t, done)
	sd := m.VideoState()
	if !sd.AudioMuted || !sd.PeerVideoOn || sd.PeerOrientation != 3 || sd.VideoUpgradeOutgoing || sd.VideoMotivo != "falha" {
		t.Fatalf("falha desfez outro estado: %+v", sd)
	}
}

func TestPeerStoppedInFlightStillEmitsAfterLocalAction(t *testing.T) {
	sock := &gatedVideoSocket{tag: "ack", entered: make(chan struct{}), release: make(chan struct{})}
	m, c := activeManager(sock)
	c.StateData.PeerVideoOn = true
	done := make(chan struct{})
	go func() { m.HandleVideoState(context.Background(), incomingVideoNode("6")); close(done) }()
	waitMediaTest(t, sock.entered)
	if _, err := m.RequestVideoUpgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(sock.release)
	waitMediaTest(t, done)
	sd := m.VideoState()
	if sd.PeerVideoOn || sd.VideoOff || sd.VideoMotivo != "peer-stopped" || sd.State != core.CallStateActive {
		t.Fatalf("stop do peer ignorado: %+v", sd)
	}
}

func TestVideoTransitionsWhileCallIsHeld(t *testing.T) {
	m, c := activeManager(&fakeSock{})
	c.StateData.PeerVideoOn = true
	if err := m.Hold(false); err != nil {
		t.Fatal(err)
	}
	m.HandleVideoState(context.Background(), incomingVideoNode("6"))
	sd := m.VideoState()
	if sd.State != core.CallStateOnHold || sd.PeerVideoOn || sd.VideoMotivo != "peer-stopped" {
		t.Fatalf("transição em espera perdida: %+v", sd)
	}
	if _, err := m.RequestVideoUpgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.StopVideo(context.Background()); err != nil {
		t.Fatal(err)
	}
	sd = m.VideoState()
	if sd.State != core.CallStateOnHold || !sd.VideoOff || sd.VideoUpgradeOutgoing || sd.VideoMotivo != "upgrade-cancelled" {
		t.Fatalf("cancelamento em espera perdido: %+v", sd)
	}
}
