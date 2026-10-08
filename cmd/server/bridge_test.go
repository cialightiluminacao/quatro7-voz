package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func makeBrowserOffer(t *testing.T) string {
	t.Helper()
	se := webrtc.SettingEngine{}
	se.SetIncludeLoopbackCandidate(true)
	se.SetInterfaceFilter(func(name string) bool { return name == "lo" })
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	mic, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "browser-mic",
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pc.AddTrack(mic); err != nil {
		t.Fatal(err)
	}
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gather
	return pc.LocalDescription().SDP
}

func TestBridgeVideoControlNegotiation(t *testing.T) {
	b := &Bridge{}
	negotiations := 0
	var received [][]byte
	b.OnVideoEnvelope = func(string) { negotiations++ }
	b.OnBrowserVideo = func(au []byte) { received = append(received, au) }
	for _, msg := range []webrtc.DataChannelMessage{
		{IsString: true, Data: []byte("Q1")},
		{IsString: true, Data: []byte("Q2")},
		{IsString: true, Data: []byte("Q1\n")},
		{IsString: true, Data: []byte("KF")},
		{Data: []byte("Q1")}, // Q1 binário é mídia, não negociação
		{Data: []byte{0, 0, 0, 1, 0x65, 1}},
		{},
	} {
		b.handleVideoMessage(msg)
	}
	if negotiations != 1 || len(received) != 2 || !bytes.Equal(received[0], []byte("Q1")) {
		t.Fatalf("Q1=%d AUs=%x", negotiations, received)
	}
}

func awaitVideoMessage[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("timeout do DataChannel local")
	}
	var zero T
	return zero
}

// Dois peers Pion locais verificam que OnMessage recebe >64 KiB, Q1 é texto,
// vídeo é binário e KF chega como texto no mesmo h264. Não acessa WhatsApp.
func TestBridgeLargeVideoAndKeyframeDataChannel(t *testing.T) {
	se := webrtc.SettingEngine{}
	se.SetIncludeLoopbackCandidate(true)
	se.SetInterfaceFilter(func(name string) bool { return name == "lo" })
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatal(err)
	}
	dc, err := pc.CreateDataChannel(h264ChannelLabel, nil)
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan bool, 1)
	dc.OnOpen(func() { opened <- true })
	messages := make(chan webrtc.DataChannelMessage, 4)
	dc.OnMessage(func(msg webrtc.DataChannelMessage) { messages <- msg })
	badChannel, err := pc.CreateDataChannel("other", nil)
	if err != nil {
		t.Fatal(err)
	}
	badOpened := make(chan bool, 1)
	badChannel.OnOpen(func() { badOpened <- true })
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gather
	b, answer, err := newBridgeWithAPI(pc.LocalDescription().SDP, slog.Default(), api)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	negotiated := make(chan bool, 1)
	received := make(chan []byte, 2)
	b.OnVideoEnvelope = func(string) { negotiated <- true }
	b.OnBrowserVideo = func(au []byte) { received <- au }
	if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatal(err)
	}
	awaitVideoMessage(t, opened)
	awaitVideoMessage(t, badOpened)
	if err = badChannel.SendText("Q1"); err != nil {
		t.Fatal(err)
	}
	if err = badChannel.Send([]byte{0, 0, 0, 1, 0x65, 1}); err != nil {
		t.Fatal(err)
	}
	// Antes de Q1: saída binária sem transformação pelo bridge.
	raw := []byte{0, 0, 0, 1, 0x65, 1}
	if err = b.WriteVideo(raw); err != nil {
		t.Fatal(err)
	}
	msg := awaitVideoMessage(t, messages)
	if msg.IsString || !bytes.Equal(msg.Data, raw) {
		t.Fatal("Annex-B legado alterado")
	}
	if err = dc.SendText("Q1"); err != nil {
		t.Fatal(err)
	}
	awaitVideoMessage(t, negotiated)
	au := append([]byte{0, 0, 0, 1, 0x65}, bytes.Repeat([]byte{7}, 120*1024)...)
	if err = dc.Send(au); err != nil {
		t.Fatal(err)
	}
	if got := awaitVideoMessage(t, received); !bytes.Equal(got, au) {
		t.Fatalf("AU recebida: %d, quero %d", len(got), len(au))
	}
	// No peer que negocia, o pipeline produz o envelope; o bridge preserva-o.
	envelope := append([]byte{0x51, 1, 3, 1, 0, 0, 0, 1, 0, 0, 0, 9}, au...)
	if err = b.WriteVideo(envelope); err != nil {
		t.Fatal(err)
	}
	msg = awaitVideoMessage(t, messages)
	if msg.IsString || !bytes.Equal(msg.Data, envelope) {
		t.Fatal("envelope binário alterado")
	}
	if err = b.RequestVideoKeyframe(); err != nil {
		t.Fatal(err)
	}
	msg = awaitVideoMessage(t, messages)
	if !msg.IsString || string(msg.Data) != "KF" {
		t.Fatalf("KF=%q texto=%v", msg.Data, msg.IsString)
	}
	select {
	case <-negotiated:
		t.Fatal("outro canal negociou Q1")
	default:
	}
	select {
	case <-received:
		t.Fatal("outro canal encaminhou mídia")
	default:
	}
}

func TestNewBridgeNegotiatesBrowserOffer(t *testing.T) {
	offer := makeBrowserOffer(t)

	br, answer, err := NewBridge(offer, slog.Default())
	if err != nil {
		t.Fatalf("NewBridge failed: %v", err)
	}
	defer br.Close()

	if answer == "" || !strings.Contains(answer, "opus") {
		t.Fatalf("answer missing or has no opus codec:\n%s", answer)
	}

	if strings.Contains(answer, "m=audio 0 ") {
		t.Fatalf("answer rejected an audio m-line:\n%s", answer)
	}
	t.Logf("bridge negotiated OK; answer has %d m-lines", strings.Count(answer, "m=audio"))
}
