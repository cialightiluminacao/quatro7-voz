package call

import (
	"testing"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

func TestBrowserKFFencedByCallAndBridgeEpoch(t *testing.T) {
	m, _ := audioTestManager(t, &guardedTestCodec{})
	defer m.cleanupMedia()
	m.currentCall.StateData.State = core.CallStateActive
	k := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := m.video.Setup(testIncomingCallID, "999:1@lid", "111:1@lid", k, k); err != nil {
		t.Fatal(err)
	}
	peer, err := media.NewSrtpSession(k, k, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	frames := 0
	m.OnPeerVideo = func([]byte) { frames++ }
	receive := func(seq uint16, nalu byte) {
		t.Helper()
		h := media.NewRtpHeader(97, seq, uint32(seq), 42)
		h.Marker = true
		b, err := peer.Protect(&media.RtpPacket{Header: h, Payload: []byte{nalu, 1}})
		if err != nil {
			t.Fatal(err)
		}
		m.onRelayData(b)
	}
	old := m.ResetBrowserMedia(true)
	current := m.ResetBrowserMedia(true)
	receive(1, 0x65)
	m.RequestPeerVideoKeyframeForBridge(testIncomingCallID, old)
	m.RequestPeerVideoKeyframeForBridge("other-call", current)
	m.RequestPeerVideoKeyframeForBridge(testIncomingCallID, 0)
	receive(2, 0x41)
	if frames != 2 {
		t.Fatal("bridge velha/call errada interferiu no decoder")
	}
	m.RequestPeerVideoKeyframeForBridge(testIncomingCallID, current)
	receive(3, 0x41)
	if frames != 2 {
		t.Fatal("KF atual não aguardou IDR")
	}
	receive(4, 0x65)
	if frames != 3 {
		t.Fatal("KF não recuperou com IDR")
	}
	m.ResetBrowserMedia(false)
	m.RequestPeerVideoKeyframeForBridge(testIncomingCallID, current)
	receive(5, 0x41)
	if frames != 4 {
		t.Fatal("bridge desconectada interferiu")
	}
}

func TestRelayDemuxAuthenticatesRTCPBeforeRTP(t *testing.T) {
	m, _ := audioTestManager(t, &guardedTestCodec{})
	defer m.cleanupMedia()
	k := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := m.video.Setup(testIncomingCallID, "999:1@lid", "111:1@lid", k, k); err != nil {
		t.Fatal(err)
	}
	peer, err := media.NewSrtcpSession(k, k)
	if err != nil {
		t.Fatal(err)
	}
	target := media.GenerateSecureSsrc(testIncomingCallID, "999:1@lid", 2)
	b, err := peer.Protect(media.BuildVideoPLI(42, target))
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	m.OnVideoKeyframeRequest = func() { requests++ }
	m.onRelayData(media.BuildVideoPLI(42, target)) // RTCP em claro não autenticado
	if requests != 0 {
		t.Fatal("RTCP sem auth aceito")
	}
	m.onRelayData(b)
	m.onRelayData(b)
	if requests != 1 {
		t.Fatal("SRTCP perdeu no demux ou duplicou callback")
	}
}
