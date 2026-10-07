package transport

import (
	"testing"

	"github.com/pion/webrtc/v4"
)

func TestRelaySendErrorsCountMediaAndDrain(t *testing.T) {
	m := NewSctpRelayManager(nil)
	// Um DataChannel ainda fechado devolve erro real do pion, sem conexão externa.
	c := &relayConnection{state: relayStateOpen, channel: &webrtc.DataChannel{}}
	m.connections["test"] = c
	if err := m.sendRaw(c, []byte{1}); err == nil {
		t.Fatal("DataChannel fechado deveria falhar")
	}
	if got := m.DrainSendErrors(); got != 0 {
		t.Fatalf("envio de controle contaminou contador de mídia: %d", got)
	}
	m.Broadcast([]byte{1})
	m.Broadcast([]byte{2})
	if got := m.DrainSendErrors(); got != 2 {
		t.Fatalf("send errors=%d, quer 2", got)
	}
	if got := m.DrainSendErrors(); got != 0 {
		t.Fatalf("drenagem repetiu erros: %d", got)
	}
}
