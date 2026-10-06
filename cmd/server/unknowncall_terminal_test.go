package main

import (
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
)

// [frota6] 2998176: <terminate>/<reject> que chega junto com outros nós (o whatsmeow
// entrega como UnknownCallEvent) precisa ser reconhecido como terminal, em qualquer
// posição; nós não terminais (relaylatency, video...) não.
func TestNodeHasTerminalCall(t *testing.T) {
	mk := func(tags ...string) *waBinary.Node {
		n := &waBinary.Node{Tag: "call"}
		var kids []waBinary.Node
		for _, tg := range tags {
			kids = append(kids, waBinary.Node{Tag: tg})
		}
		n.Content = kids
		return n
	}
	casos := []struct {
		nome string
		node *waBinary.Node
		quer bool
	}{
		{"terminate depois de relaylatency", mk("relaylatency", "terminate"), true},
		{"reject sozinho", mk("reject"), true},
		{"só relaylatency", mk("relaylatency"), false},
		{"video", mk("video"), false},
		{"sem filhos", mk(), false},
	}
	for _, c := range casos {
		if got := nodeHasTerminalCall(c.node); got != c.quer {
			t.Errorf("%s: nodeHasTerminalCall=%v, quer %v", c.nome, got, c.quer)
		}
	}
}
