package call

import (
	"context"
	"reflect"
	"testing"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func orientationNode(callID string, value any) *waBinary.Node {
	attrs := waBinary.Attrs{"call-id": callID}
	if value != nil {
		attrs["device_orientation"] = value
	}
	return &waBinary.Node{Tag: "call", Content: []waBinary.Node{{Tag: "video", Attrs: attrs}}}
}

func TestPeerOrientationStanzaFallback(t *testing.T) {
	for _, held := range []bool{false, true} {
		m, c := activeManager(&fakeSock{})
		m.video.BeginCall(c.CallID)
		if held {
			c.StateData.State = core.CallStateOnHold
		}
		var orientations []int
		m.OnVideoStateChanged = func(c *CallInfo) {
			if c.StateData.VideoMotivo != "peer-orientation" {
				t.Errorf("motivo=%q", c.StateData.VideoMotivo)
			}
			orientations = append(orientations, c.StateData.PeerOrientation)
		}
		// Stanza só de orientação, sem state; inclusive volta a zero.
		for _, o := range []any{"1", 2, "3", "0", "0"} {
			m.HandleVideoState(context.Background(), orientationNode("CID", o))
		}
		for _, o := range []any{nil, "", "x", "-1", "4", "90", "1.5"} {
			m.HandleVideoState(context.Background(), orientationNode("CID", o))
		}
		m.HandleVideoState(context.Background(), orientationNode("OUTRA", 2))
		if !reflect.DeepEqual(orientations, []int{1, 2, 3, 0}) || m.VideoState().PeerOrientation != 0 {
			t.Fatal(orientations)
		}
		c.StateData.State = core.CallStateEnded
		m.HandleVideoState(context.Background(), orientationNode("CID", 1))
		if len(orientations) != 4 {
			t.Fatal("stanza alterou chamada encerrada")
		}
	}
}

func TestPeerOrientationBeforeActiveCallIsRetained(t *testing.T) {
	m, c := activeManager(&fakeSock{})
	m.video.BeginCall(c.CallID)
	c.StateData.State = core.CallStateConnecting
	m.HandleVideoState(context.Background(), orientationNode("CID", 2))
	if sd := m.VideoState(); sd.State != core.CallStateConnecting || sd.PeerOrientation != 2 || sd.VideoMotivo != "peer-orientation" {
		t.Fatalf("fallback inicial=%+v", sd)
	}
	m.notePeerVideoOrientation("CID", 3)
	c.StateData.State = core.CallStateActive
	if m.VideoState().PeerOrientation != 3 {
		t.Fatal("CVO recebido antes de active foi perdido")
	}
}

func TestPeerOrientationRTPPrecedenceAndSnapshot(t *testing.T) {
	m, c := activeManager(&fakeSock{})
	m.video.BeginCall(c.CallID)
	var events []CallStateData
	m.OnVideoStateChanged = func(c *CallInfo) { events = append(events, c.StateData) }
	m.HandleVideoState(context.Background(), orientationNode("CID", 2))
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := m.video.Setup("CID", "999:1@lid", "111:1@lid", km, km); err != nil {
		t.Fatal(err)
	}
	peer, err := media.NewSrtpSession(km, km, core.SRTPRecvAuthTagLen, core.SRTPSendAuthTagLen)
	if err != nil {
		t.Fatal(err)
	}
	var frames [][]byte
	m.OnPeerVideo = func(au []byte) { frames = append(frames, au) }
	receive := func(seq uint16, o *byte) {
		t.Helper()
		h := media.NewRtpHeader(core.PayloadTypeWhatsAppH264, seq, uint32(seq), 42)
		h.Marker = true
		if o != nil {
			h.Extension, h.ExtensionProfile, h.ExtensionData = true, 0xDEBE, []byte{0x30, 0x08 | *o, 0, 0}
		}
		data, err := peer.Protect(&media.RtpPacket{Header: h, Payload: []byte{0x65, 1}})
		if err != nil {
			t.Fatal(err)
		}
		m.video.HandleRelayData(data)
	}
	// Q1 pode chegar antes de Setup; stanza deve continuar no envelope.
	m.EnableVideoEnvelope(m.currentCall.CallID)
	receive(1, nil)
	if len(frames) != 1 || frames[0][2] != 2 || m.VideoState().PeerOrientation != 2 {
		t.Fatal("fallback não chegou ao browser")
	}
	o := byte(3)
	receive(2, &o)
	m.HandleVideoState(context.Background(), orientationNode("CID", 1))
	receive(3, nil)
	receive(4, &o) // mesmo CVO não emite outra mudança
	if sd := m.VideoState(); sd.PeerOrientation != 3 || sd.VideoMotivo != "peer-orientation" {
		t.Fatalf("snapshot=%+v", sd)
	}
	if frames[1][2] != 3 || frames[2][2] != 3 {
		t.Fatal("stanza sobrepôs o CVO no envelope")
	}
	var changed []int
	for _, event := range events {
		if event.VideoMotivo == "peer-orientation" {
			changed = append(changed, event.PeerOrientation)
		}
	}
	if !reflect.DeepEqual(changed, []int{2, 3}) {
		t.Fatal(changed)
	}
	// Callback de uma chamada anterior não altera a atual.
	m.notePeerVideoOrientation("ANTIGA", 0)
	if m.VideoState().PeerOrientation != 3 {
		t.Fatal("callback antigo aplicado")
	}
}
