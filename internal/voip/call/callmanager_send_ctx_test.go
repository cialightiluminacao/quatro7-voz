package call

import (
	"context"
	"sync"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
)

// ctxSock imita o whatsmeow (NoiseSocket.SendFrame começa com
// `if ctx.Err() != nil { return ctx.Err() }`): recusa envio com ctx cancelado e
// só registra os envios bem-sucedidos. O envio fica preso em `gate` até o teste
// liberar — reproduz a goroutine que roda DEPOIS do handler HTTP responder.
type ctxSock struct {
	fakeSock
	gate chan struct{}
	mu2  sync.Mutex
	ok   []waBinary.Node
}

func (f *ctxSock) send(ctx context.Context, node waBinary.Node) error {
	<-f.gate
	if ctx.Err() != nil {
		return ctx.Err()
	}
	f.mu2.Lock()
	f.ok = append(f.ok, node)
	f.mu2.Unlock()
	return nil
}

func (f *ctxSock) SendNode(ctx context.Context, node waBinary.Node) error {
	return f.send(ctx, node)
}

func (f *ctxSock) Query(ctx context.Context, node waBinary.Node) (*waBinary.Node, error) {
	return nil, f.send(ctx, node)
}

func (f *ctxSock) waitChild(t *testing.T, child string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu2.Lock()
		for _, n := range f.ok {
			for _, c := range n.GetChildren() {
				if c.Tag == child {
					f.mu2.Unlock()
					return
				}
			}
		}
		f.mu2.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("<%s> nunca saiu pelo socket com ctx válido (ctx da requisição cancelado engoliu o envio)", child)
}

func ringingWithCtxSock() (*CallManager, *ctxSock) {
	m := incomingRingingManager()
	s := &ctxSock{gate: make(chan struct{})}
	m.sock = s
	return m, s
}

// Recusar pelo HTTP: o handler responde 200 e o net/http cancela r.Context().
// O <reject> tem de sair mesmo assim.
func TestRejectCallEnviaMesmoComCtxDaRequisicaoCancelado(t *testing.T) {
	m, s := ringingWithCtxSock()
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.RejectCall(ctx, testIncomingCallID, "declined"); err != nil {
		t.Fatal(err)
	}
	cancel() // handler retornou
	close(s.gate)
	s.waitChild(t, "reject")
}

// Desligar pelo HTTP (DELETE): idem para o <terminate>.
func TestEndCallEnviaMesmoComCtxDaRequisicaoCancelado(t *testing.T) {
	m, s := ringingWithCtxSock()
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.EndCall(ctx, "user_ended"); err != nil {
		t.Fatal(err)
	}
	cancel()
	close(s.gate)
	s.waitChild(t, "terminate")
}
