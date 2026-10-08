package media

import (
	"bytes"

	"github.com/pion/srtp/v3"
	"wacalls/internal/voip/core"
)

// SRTCP usa os mesmos masters por JID do SRTP, labels 3/4/5 e tag de 10 bytes.
// Referência: meowcaller c48c3e2, srtp/e2e.go:95-109,190-235.
// Pion implementa AES-CM/HMAC-SHA1, índices por SSRC e proteção contra replay.
// O chamador serializa Protect/Unprotect.
type SrtcpSession struct {
	send, recv     *srtp.Context
	sendKM, recvKM core.SrtpKeyingMaterial
}

func NewSrtcpSession(send, recv core.SrtpKeyingMaterial) (*SrtcpSession, error) {
	sc, err := srtp.CreateContext(send.MasterKey, send.MasterSalt, srtp.ProtectionProfileAes128CmHmacSha1_80)
	if err != nil {
		return nil, err
	}
	rc, err := srtp.CreateContext(recv.MasterKey, recv.MasterSalt, srtp.ProtectionProfileAes128CmHmacSha1_80, srtp.SRTCPReplayProtection(128))
	if err != nil {
		return nil, err
	}
	clone := func(k core.SrtpKeyingMaterial) core.SrtpKeyingMaterial {
		return core.SrtpKeyingMaterial{MasterKey: bytes.Clone(k.MasterKey), MasterSalt: bytes.Clone(k.MasterSalt)}
	}
	return &SrtcpSession{send: sc, recv: rc, sendKM: clone(send), recvKM: clone(recv)}, nil
}

func sameSrtcpMaster(a, b core.SrtpKeyingMaterial) bool {
	return bytes.Equal(a.MasterKey, b.MasterKey) && bytes.Equal(a.MasterSalt, b.MasterSalt)
}

// Cada direção tem masters independentes: rekey só de recepção não pode
// reutilizar o índice/IV de envio, e rekey só de envio não apaga replay recebido.
func (s *SrtcpSession) PreserveUnchangedKeys(previous *SrtcpSession) {
	if previous == nil {
		return
	}
	if sameSrtcpMaster(s.sendKM, previous.sendKM) {
		s.send = previous.send
	}
	if sameSrtcpMaster(s.recvKM, previous.recvKM) {
		s.recv = previous.recv
	}
}

func (s *SrtcpSession) Protect(packet []byte) ([]byte, error) {
	return s.send.EncryptRTCP(nil, packet, nil)
}
func (s *SrtcpSession) Unprotect(packet []byte) ([]byte, error) {
	return s.recv.DecryptRTCP(nil, packet, nil)
}
