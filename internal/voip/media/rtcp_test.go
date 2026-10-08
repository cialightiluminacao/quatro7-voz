package media

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"wacalls/internal/voip/core"
)

func rtcpTestKeys() core.SrtpKeyingMaterial {
	k := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	for i := range k.MasterKey {
		k.MasterKey[i] = byte(i)
	}
	for i := range k.MasterSalt {
		k.MasterSalt[i] = byte(i)
	}
	return k
}

func TestSrtcpVideoPLIKnownVectorAndReplay(t *testing.T) {
	k := rtcpTestKeys()
	s, err := NewSrtcpSession(k, k)
	if err != nil {
		t.Fatal(err)
	}
	plain := BuildVideoPLI(0x55667788, 0xaabbccdd)
	wantPlain, _ := hex.DecodeString("91ce000255667788aabbccdd")
	if !bytes.Equal(plain, wantPlain) {
		t.Fatalf("PLI=%x", plain)
	}
	// Vetor calculado independentemente com AES-CTR/HMAC (Python cryptography),
	// labels 3/4/5 e IV do meowcaller c48c3e2 srtp/e2e.go:190-211, índice 1.
	want, _ := hex.DecodeString("91ce0002556677886bc2a8f580000001db5d0a050e7c21e60491")
	packet, err := s.Protect(plain)
	if err != nil || !bytes.Equal(packet, want) {
		t.Fatalf("SRTCP=%x err=%v", packet, err)
	}
	for _, i := range []int{0, 4, 8, 12, len(packet) - 1} {
		bad := bytes.Clone(packet)
		bad[i] ^= 1
		if _, err := s.Unprotect(bad); err == nil {
			t.Fatalf("aceitou adulteração byte %d", i)
		}
	}
	got, err := s.Unprotect(packet)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("RTCP=%x err=%v", got, err)
	}
	if _, err := s.Unprotect(packet); err == nil {
		t.Fatal("aceitou replay do outro relay")
	}
	if !sameSrtcpMaster(s.sendKM, k) || !sameSrtcpMaster(s.recvKM, k) {
		t.Fatal("masters iguais não reconhecidos")
	}
	other := rtcpTestKeys()
	other.MasterKey[0] ^= 1
	if sameSrtcpMaster(s.sendKM, other) {
		t.Fatal("masters diferentes reconhecidos")
	}
	if _, err := NewSrtcpSession(core.SrtpKeyingMaterial{}, k); err == nil {
		t.Fatal("aceitou master vazio")
	}
}

func TestVideoCompoundBytesAndIndependentSrtcpVector(t *testing.T) {
	plain := BuildVideoKeyframeRequest(0x55667788, 0xaabbccdd)
	wantPlain, _ := hex.DecodeString("90c900015566778891ca0007556677880112353536363740706a3636373738382e6f72670000000091ce000255667788aabbccdd")
	if !bytes.Equal(plain, wantPlain) {
		t.Fatalf("RR+SDES+PLI=%x", plain)
	}
	infos, ok := ParseRTCP(plain, 0xaabbccdd)
	if !ok || len(infos) != 3 || infos[0].Type != 201 || infos[1].Type != 202 || !infos[2].RequestsKeyframe {
		t.Fatalf("compound=%v valid=%v", infos, ok)
	}
	// AES-CTR e HMAC-SHA1 calculados independentemente com Python cryptography:
	// labels 3/4/5, sender do RR em claro, compound inteiro após byte 8 cifrado.
	want, _ := hex.DecodeString("90c900015566778850b3642f4a1c78465d86b8464fb333eaf7eeafa23acac68912e34956cec9f3932ac009e58628c84a89e8377f80000001255cc6264922b27d2f1f")
	s, err := NewSrtcpSession(rtcpTestKeys(), rtcpTestKeys())
	if err != nil {
		t.Fatal(err)
	}
	packet, err := s.Protect(plain)
	if err != nil || !bytes.Equal(packet, want) {
		t.Fatalf("SRTCP=%x err=%v", packet, err)
	}
	for _, i := range []int{0, 4, 8, 16, len(packet) - 14, len(packet) - 1} {
		bad := bytes.Clone(packet)
		bad[i] ^= 1
		if _, err := s.Unprotect(bad); err == nil {
			t.Fatalf("aceitou byte adulterado %d", i)
		}
	}
	got, err := s.Unprotect(packet)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("roundtrip=%x err=%v", got, err)
	}
	if _, err := s.Unprotect(packet); err == nil {
		t.Fatal("compound replay aceito")
	}
}

func TestSrtcpReorderedIndicesAndUnencryptedRTCP(t *testing.T) {
	k := rtcpTestKeys()
	s, _ := NewSrtcpSession(k, k)
	var packets [][]byte
	for i := 0; i < 3; i++ {
		b, err := s.Protect(BuildVideoPLI(42, 50))
		if err != nil {
			t.Fatal(err)
		}
		packets = append(packets, b)
	}
	for _, i := range []int{2, 0, 1} {
		if _, err := s.Unprotect(packets[i]); err != nil {
			t.Fatalf("índice fora de ordem: %v", err)
		}
	}
	if _, err := s.Unprotect(BuildVideoPLI(42, 50)); err == nil {
		t.Fatal("aceitou RTCP em claro sem auth")
	}
}

func TestRTCPClassificationAndFeedback(t *testing.T) {
	for _, b := range [][]byte{nil, {0x80, 97, 0, 0, 0, 0, 0, 0}, {0x80, 225, 0, 0, 0, 0, 0, 0}, {0, 206, 0, 0, 0, 0, 0, 0}} {
		if IsRTCPPacket(b) {
			t.Fatalf("classificou RTP/STUN como RTCP: %x", b)
		}
	}
	pli := BuildVideoPLI(42, 50)
	for _, first := range []byte{0x81, 0x91} {
		pli[0] = first
		info, ok := ParseRTCP(pli, 50)
		if !IsRTCPPacket(pli) || !ok || len(info) != 1 || !info[0].RequestsKeyframe {
			t.Fatalf("PLI=%x info=%v", pli, info)
		}
		info, ok = ParseRTCP(pli, 51)
		if !ok || info[0].RequestsKeyframe {
			t.Fatal("PLI de outro stream")
		}
	}
	fir := []byte{0x94, 206, 0, 4, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 50, 1, 0, 0, 0}
	info, ok := ParseRTCP(fir, 50)
	if !ok || !info[0].RequestsKeyframe {
		t.Fatal("FIR com media=0 e alvo no FCI")
	}
	info, ok = ParseRTCP(fir, 51)
	if !ok || info[0].RequestsKeyframe {
		t.Fatal("FIR de outro stream")
	}
	// Compound com RR + PLI, e tipos nativos 208/209 sem feedback de chave.
	rr := []byte{0x80, 201, 0, 1, 0, 0, 0, 42}
	info, ok = ParseRTCP(append(bytes.Clone(rr), pli...), 50)
	if !ok || len(info) != 2 || !info[1].RequestsKeyframe {
		t.Fatal("compound RR+PLI")
	}
	for _, pt := range []byte{200, 202, 204, 205, 208, 209} {
		b := bytes.Clone(rr)
		b[1] = pt
		info, ok = ParseRTCP(b, 50)
		if !ok || info[0].RequestsKeyframe {
			t.Fatalf("PT %d pediu chave", pt)
		}
	}
	for _, bad := range [][]byte{nil, pli[:11], append(bytes.Clone(pli), 1), append(bytes.Clone(pli), 0x80, 206, 0, 3), fir[:16]} {
		if info, ok := ParseRTCP(bad, 50); ok || info != nil {
			t.Fatalf("aceitou compound malformado %x", bad)
		}
	}
	bad := bytes.Clone(pli)
	binary.BigEndian.PutUint16(bad[2:4], 0xffff)
	if _, ok := ParseRTCP(bad, 50); ok {
		t.Fatal("length fora dos bytes")
	}
}

func TestSrtcpRekeyPreservesUnchangedDirection(t *testing.T) {
	k := rtcpTestKeys()
	other := rtcpTestKeys()
	other.MasterKey[0] ^= 1
	s, err := NewSrtcpSession(k, k)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Protect(BuildVideoPLI(42, 50))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Unprotect(b); err != nil {
		t.Fatal(err)
	}
	for _, keys := range [][2]core.SrtpKeyingMaterial{{k, other}, {k, k}, {other, k}} {
		next, err := NewSrtcpSession(keys[0], keys[1])
		if err != nil {
			t.Fatal(err)
		}
		oldSend, oldRecv := s.send, s.recv
		next.PreserveUnchangedKeys(s)
		if (next.send == oldSend) != sameSrtcpMaster(s.sendKM, keys[0]) || (next.recv == oldRecv) != sameSrtcpMaster(s.recvKM, keys[1]) {
			t.Fatal("rekey não preservou direção intacta")
		}
		s = next
	}
}
