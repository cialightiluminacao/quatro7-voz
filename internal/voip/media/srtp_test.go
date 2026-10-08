package media

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"wacalls/internal/voip/core"
)

func TestSrtpRejectsForgeryWithoutChangingReceiveState(t *testing.T) {
	key := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	tx, err := NewSrtpSession(key, key, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	rx, err := NewSrtpSession(key, key, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint16{65534, 65535, 0, 1} {
		packet, err := tx.Protect(&RtpPacket{Header: NewRtpHeader(111, seq, 0, 20), Payload: []byte{1, 2}})
		if err != nil {
			t.Fatal(err)
		}
		for _, offset := range []int{2, 8, 12, len(packet) - 1} {
			forged := bytes.Clone(packet)
			if offset == 2 {
				binary.BigEndian.PutUint16(forged[2:], 30000)
			} else {
				forged[offset] ^= 0xff
			}
			previous := *rx.recvCtx
			_, err := rx.Unprotect(forged)
			var srtpErr *SrtpError
			if !errors.As(err, &srtpErr) || srtpErr.Type != SrtpErrAuthFailed {
				t.Fatalf("seq=%d alteração=%d: esperava falha de autenticação", seq, offset)
			}
			if rx.recvCtx.roc != previous.roc || rx.recvCtx.lastSeq != previous.lastSeq || rx.recvCtx.ssrc != previous.ssrc || rx.recvCtx.initialized != previous.initialized {
				t.Fatal("falha de autenticação alterou ROC/seq/SSRC")
			}
		}
		decoded, err := rx.Unprotect(packet)
		if err != nil || !bytes.Equal(decoded.Payload, []byte{1, 2}) {
			t.Fatalf("áudio real após forjado: seq=%d err=%v", seq, err)
		}
	}
}
