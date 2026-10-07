package call

import (
	"context"
	"sync"
	"testing"

	"wacalls/internal/voip/core"
)

func TestRingTimeoutCannotRejectAcceptedCall(t *testing.T) {
	m := incomingRingingManager()
	c := &guardedTestCodec{}
	m.codec = c
	if err := m.AcceptCall(context.Background(), testIncomingCallID); err != nil {
		t.Fatal(err)
	}
	if err := m.RejectCall(context.Background(), testIncomingCallID, core.EndCallReasonTimeout); err == nil {
		t.Fatal("timeout conseguiu recusar chamada aceita")
	}
	if c.closed.Load() || m.CurrentCall().StateData.State != core.CallStateConnecting {
		t.Fatal("timeout alterou/limpou a chamada aceita")
	}
	m.cleanupMedia()
}

func TestRingTimeoutAndAcceptanceHaveOneWinner(t *testing.T) {
	for range 100 {
		m := incomingRingingManager()
		c := &guardedTestCodec{}
		m.codec = c
		start := make(chan struct{})
		var acceptErr, rejectErr error
		var wg sync.WaitGroup
		wg.Go(func() { <-start; acceptErr = m.AcceptCall(context.Background(), testIncomingCallID) })
		wg.Go(func() {
			<-start
			rejectErr = m.RejectCall(context.Background(), testIncomingCallID, core.EndCallReasonTimeout)
		})
		close(start)
		wg.Wait()
		if (acceptErr == nil) == (rejectErr == nil) {
			t.Fatalf("accept=%v reject=%v: deve haver exatamente um vencedor", acceptErr, rejectErr)
		}
		if acceptErr == nil {
			if c.closed.Load() || m.CurrentCall().StateData.State != core.CallStateConnecting {
				t.Fatal("aceite venceu mas foi encerrado pelo timeout")
			}
		} else if !c.closed.Load() || m.CurrentCall().StateData.EndReason != core.EndCallReasonTimeout {
			t.Fatal("timeout venceu mas não encerrou a chamada")
		}
		m.cleanupMedia()
	}
}
