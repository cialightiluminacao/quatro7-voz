package main

import "testing"

// Quem abre o SSE no meio do pareamento precisa receber o QR atual; sem isso o
// painel pedia /pair de novo e trocava o QR que o operador já estava lendo.
func TestQRSnapshotsSoSessoesEmQR(t *testing.T) {
	m := &SessionManager{sessions: map[string]*Session{
		"a": {id: "a", auth: AuthSnapshot{State: "qr", QR: "2@abc"}},
		"b": {id: "b", auth: AuthSnapshot{State: "open", Paired: true}},
		"c": {id: "c", auth: AuthSnapshot{State: "qr"}}, // ainda sem código
	}}
	evs := m.qrSnapshots()
	if len(evs) != 1 {
		t.Fatalf("esperava 1 auth-state, veio %d: %v", len(evs), evs)
	}
	ev := evs[0].(map[string]any)
	if ev["type"] != "auth-state" || ev["sessionId"] != "a" || ev["qr"] != "2@abc" {
		t.Fatalf("evento errado: %v", ev)
	}
}
