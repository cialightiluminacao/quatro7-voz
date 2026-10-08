package call

import (
	"context"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

func TestBridgeSwapResetsAudioAndEnvelopeKeepsVideoSnapshot(t *testing.T) {
	m, tx := audioTestManager(t, &guardedTestCodec{})
	defer m.cleanupMedia()
	m.currentCall.StateData.State = core.CallStateActive
	m.currentCall.StateData.PeerOrientation = 2
	m.currentCall.StateData.PeerVideoOn = true
	m.currentCall.StateData.VideoUpgradeOutgoing = true
	before := m.VideoState()
	oldEpoch := m.ResetBrowserMedia(true)
	m.EnableVideoEnvelopeForBridge(testIncomingCallID, oldEpoch)
	// A capture in flight and a peer reorder timer must not survive B's attachment.
	m.FeedBrowserPCM(oldEpoch, make([]float32, frame16))
	m.handleAudioRelayData(audioTestPacket(t, tx, 1, 0))
	m.handleAudioRelayData(audioTestPacket(t, tx, 3, 2*frame16))
	m.mu.Lock()
	oldReceiveEpoch := m.audioReceiveEpoch
	hadPending := len(m.audioReorder.pending) > 0 && m.audioReorderTimer != nil
	m.mu.Unlock()
	if !hadPending {
		t.Fatal("fixture did not arm reorder timer")
	}
	newEpoch := m.ResetBrowserMedia(true)
	m.FeedBrowserPCM(oldEpoch, make([]float32, frame16)) // decoded by A before the swap
	m.mu.Lock()
	clean := len(m.captureBuf) == 0 && len(m.audioReorder.pending) == 0 && m.audioReorderTimer == nil && !m.audioTimelineSet && m.audioPlayedSamples == 0 && m.audioReceiveEpoch > oldReceiveEpoch
	m.mu.Unlock()
	if !clean || newEpoch <= oldEpoch || m.VideoState() != before {
		t.Fatal("swap retained audio or changed W1 video state")
	}
	m.FeedBrowserPCM(newEpoch, make([]float32, frame16))
	m.mu.Lock()
	newCapture := len(m.captureBuf)
	m.mu.Unlock()
	if newCapture != frame16 {
		t.Fatal("new browser capture blocked")
	}
	// Inspect actual received video bytes: B begins as Annex-B, then negotiates Q1.
	key := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := m.video.Setup(testIncomingCallID, "999:1@lid", "111:1@lid", key, key); err != nil {
		t.Fatal(err)
	}
	peer, err := media.NewSrtpSession(key, key, core.SRTPRecvAuthTagLen, core.SRTPSendAuthTagLen)
	if err != nil {
		t.Fatal(err)
	}
	var frames [][]byte
	m.OnPeerVideo = func(au []byte) { frames = append(frames, au) }
	receive := func(seq uint16) {
		h := media.NewRtpHeader(core.PayloadTypeWhatsAppH264, seq, uint32(seq), 42)
		h.Marker = true
		data, err := peer.Protect(&media.RtpPacket{Header: h, Payload: []byte{0x65, 1}})
		if err != nil {
			t.Fatal(err)
		}
		m.video.HandleRelayData(data)
	}
	m.EnableVideoEnvelopeForBridge(testIncomingCallID, oldEpoch) // old Q1 arrived after swap
	m.EnableVideoEnvelopeForBridge("another-call", newEpoch)
	receive(1)
	if len(frames) != 1 || frames[0][0] != 0 {
		t.Fatal("old bridge/call enabled Q1 for B")
	}
	m.EnableVideoEnvelopeForBridge(testIncomingCallID, newEpoch)
	receive(2)
	if len(frames) != 2 || frames[1][0] != 0x51 || frames[1][1] != 1 {
		t.Fatal("new Q1 not applied")
	}
	// A packet already formatted as Q1 must finish delivery before reset. Otherwise
	// that envelope could be delivered to a replacement browser expecting Annex-B.
	entered, release, received, reset := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	m.OnPeerVideo = func([]byte) { close(entered); <-release }
	h := media.NewRtpHeader(core.PayloadTypeWhatsAppH264, 3, 3, 42)
	h.Marker = true
	data, err := peer.Protect(&media.RtpPacket{Header: h, Payload: []byte{0x65, 1}})
	if err != nil {
		t.Fatal(err)
	}
	go func() { m.onRelayData(data); close(received) }()
	waitMediaTest(t, entered)
	go func() { m.ResetBrowserMedia(true); close(reset) }()
	select {
	case <-reset:
		t.Error("reset overtook video callback")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	waitMediaTest(t, received)
	waitMediaTest(t, reset)
	// An ICE/WS close queued before DisableTerminate cannot end B's call.
	m.EndBrowserCall(context.Background(), core.EndCallReasonUserEnded, oldEpoch)
	if m.VideoState().State != core.CallStateActive {
		t.Fatal("old terminal callback ended B's call")
	}
}

func TestPlaybackEncodeFailureDoesNotSkipSamples(t *testing.T) {
	codec := &guardedTestCodec{encodeErr: &CallError{"test encode failure"}}
	m, _ := audioTestManager(t, codec)
	defer m.cleanupMedia()
	m.clock = &fakePlaybackClock{}
	m.rtpSession = media.NewWhatsAppOpusSession(10)
	m.relay = &statsTestRelay{}
	m.currentCall.StateData.State = core.CallStateActive
	m.mu.Lock()
	_, err := m.newPlaybackLocked(media.WAVPCM{Samples: []float32{.4}, Rate: 16000}, false, true, time.Second)
	if err != nil {
		m.mu.Unlock()
		t.Fatal(err)
	}
	m.sendMediaFrameLocked(make([]float32, frame16))
	pos, finished := m.playback.pos, m.playback.finishTimer != nil
	m.mu.Unlock()
	if pos != 0 || finished {
		t.Fatal("failed encode consumed playback or scheduled hangup")
	}
	m.mu.Lock()
	codec.encodeErr = nil
	m.sendMediaFrameLocked(make([]float32, frame16))
	finished = m.playback.finishTimer != nil
	m.mu.Unlock()
	if !finished || codec.encodedPCM[0][0] != .4 || codec.encodedPCM[1][0] != .4 {
		t.Fatal("retry did not replay the failed frame")
	}
}
