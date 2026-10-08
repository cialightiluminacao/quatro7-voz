package media

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestREMBKnownVectorAndTarget(t *testing.T) {
	// 750000 bits/s = mantissa 187500 (0x2dc6c) << exponent 2.
	base, _ := hex.DecodeString("8fce00050000002a0000000052454d42010adc6c55667788")
	for _, first := range []byte{0x8f, 0x9f} {
		base[0] = first
		for _, target := range []uint32{0, 0x55667788, 0x55667789} {
			infos, ok := ParseRTCP(base, target)
			if !ok || len(infos) != 1 || !infos[0].REMB || infos[0].REMBBitrate != 750000 || infos[0].REMBForVideo != (target == 0x55667788) || infos[0].RequestsKeyframe {
				t.Fatalf("REMB=%x target=%x info=%+v valid=%v", base, target, infos, ok)
			}
		}
	}
	// AFB desconhecido continua classificável, sem adivinhar um bitrate.
	unknown := bytes.Clone(base)
	copy(unknown[12:16], "TEST")
	infos, ok := ParseRTCP(unknown, 0x55667788)
	if !ok || infos[0].REMB {
		t.Fatal("AFB desconhecido interpretado como REMB")
	}
	zero := bytes.Clone(base)
	clear(zero[17:20])
	infos, ok = ParseRTCP(zero, 0x55667788)
	if !ok || !infos[0].REMBForVideo || infos[0].REMBBitrate != 0 {
		t.Fatal("REMB zero deve ser explícito")
	}
	// Lista com áudio + vídeo, não somente a primeira entrada.
	multi := append(bytes.Clone(base), 0x55, 0x66, 0x77, 0x88)
	multi[3], multi[16] = 6, 2
	binary.BigEndian.PutUint32(multi[20:24], 100)
	infos, ok = ParseRTCP(multi, 0x55667788)
	if !ok || !infos[0].REMBForVideo {
		t.Fatal("vídeo na segunda entrada ignorado")
	}
}

func TestREMBRejectsTruncationOverflowAndMalformedCompound(t *testing.T) {
	base, _ := hex.DecodeString("9fce00050000002a0000000052454d42010adc6c55667788")
	for _, mutate := range []func([]byte) []byte{
		func(b []byte) []byte { b[16] = 2; return b },
		func(b []byte) []byte { b[16] = 0; return b },
		func(b []byte) []byte { b[17], b[18], b[19] = 0xff, 0xff, 0xff; return b },
		func(b []byte) []byte { b[3] = 4; return b[:20] },
		func(b []byte) []byte { return append(b, 0x80, 206, 0, 10) },
	} {
		if infos, ok := ParseRTCP(mutate(bytes.Clone(base)), 0x55667788); ok || infos != nil {
			t.Fatalf("REMB/compound malformado aceito: %+v", infos)
		}
	}
}

func TestVideoSenderReportFieldsAndSrtcpRoundTrip(t *testing.T) {
	plain := BuildVideoSenderReport(0x55667788, 90001, 4, 12345, 100500)
	if len(plain) != 60 || !bytes.Equal(plain[:4], []byte{0x90, 200, 0, 6}) || !bytes.Equal(plain[28:32], []byte{0x91, 202, 0, 7}) {
		t.Fatalf("SR+SDES=%x", plain)
	}
	for off, want := range map[int]uint32{4: 0x55667788, 8: 2208988900, 12: 0x80000000, 16: 90001, 20: 4, 24: 12345, 32: 0x55667788} {
		if got := binary.BigEndian.Uint32(plain[off : off+4]); got != want {
			t.Fatalf("offset=%d got=%d want=%d", off, got, want)
		}
	}
	infos, ok := ParseRTCP(plain, 50)
	if !ok || len(infos) != 2 || infos[0].Type != 200 || infos[1].Type != 202 {
		t.Fatal(infos)
	}
	s, err := NewSrtcpSession(rtcpTestKeys(), rtcpTestKeys())
	if err != nil {
		t.Fatal(err)
	}
	packet, err := s.Protect(plain)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Unprotect(packet)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("got=%x err=%v", got, err)
	}
}
