package call

import (
	"context"
	"reflect"
	"testing"

	"wacalls/internal/voip/core"
)

// Aceitar o upgrade SEM câmera: só state=4 (aceite); nada de state=1 ("nossa câmera
// ligada"), e o estado local fica com a câmera desligada mas a chamada vira vídeo.
func TestAcceptVideoUpgradeSemCamera(t *testing.T) {
	sock := &fakeSock{}
	m, call := activeManager(sock)
	call.StateData.VideoUpgradeIncoming = true

	if _, err := m.AcceptVideoUpgradeWith(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := sock.videoStateSent(); !reflect.DeepEqual(got, []string{"4"}) {
		t.Fatalf("esperava só state=4, veio %v", got)
	}
	if !call.StateData.VideoOff {
		t.Error("VideoOff deveria ficar true (sem câmera nossa)")
	}
	if call.MediaType != core.CallMediaTypeVideo || call.StateData.VideoUpgradeIncoming {
		t.Error("chamada deveria virar vídeo e limpar o pedido")
	}
}

// Compatibilidade: AcceptVideoUpgrade continua mandando 4 e depois 1.
func TestAcceptVideoUpgradeComCameraMantemComportamento(t *testing.T) {
	sock := &fakeSock{}
	m, call := activeManager(sock)
	if err := m.AcceptVideoUpgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := sock.videoStateSent(); !reflect.DeepEqual(got, []string{"4", "1"}) {
		t.Fatalf("esperava 4 e 1, veio %v", got)
	}
	if call.StateData.VideoOff {
		t.Error("VideoOff deveria ser false")
	}
}

// Religar a câmera numa chamada que já é vídeo manda state=1, não novo upgrade.
func TestRequestVideoUpgradeReligaComState1(t *testing.T) {
	sock := &fakeSock{}
	m, call := activeManager(sock)
	call.MediaType = core.CallMediaTypeVideo
	call.StateData.VideoOff = true
	if _, err := m.RequestVideoUpgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := sock.videoStateSent(); !reflect.DeepEqual(got, []string{"1"}) {
		t.Fatalf("esperava state=1, veio %v", got)
	}
	if call.StateData.VideoOff {
		t.Error("VideoOff deveria zerar")
	}
}

// Frames do peer ligam PeerVideoOn e emitem uma única vez.
func TestNotePeerVideoActiveIdempotente(t *testing.T) {
	m, call := activeManager(&fakeSock{})
	n := 0
	m.OnVideoStateChanged = func(*CallInfo) { n++ }
	m.notePeerVideoActive()
	m.notePeerVideoActive()
	if !call.StateData.PeerVideoOn || call.MediaType != core.CallMediaTypeVideo {
		t.Error("PeerVideoOn/MediaType não atualizados")
	}
	if n != 1 {
		t.Fatalf("callback %d vezes, esperado 1", n)
	}
}
