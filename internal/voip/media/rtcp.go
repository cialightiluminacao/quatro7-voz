package media

import "encoding/binary"

// Classifica antes do demux RTP: PT RTCP é o byte inteiro, incluindo o bit 7.
// Referência: meowcaller c48c3e2, rtp/rtcp.go:26-36,375-439.
func IsRTCPPacket(data []byte) bool {
	return len(data) >= 8 && data[0]>>6 == 2 && data[1] >= 192 && data[1] <= 223
}

func BuildVideoPLI(sender, peer uint32) []byte {
	b := []byte{0x91, 206, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[4:8], sender)
	binary.BigEndian.PutUint32(b[8:12], peer)
	return b
}

type RTCPInfo struct {
	Type, Format     uint8
	RequestsKeyframe bool
}

// ParseRTCP valida os limites de TODO o compound e o feedback de chave antes de
// permitir qualquer efeito. Não interpreta o corpo dos relatórios SR/RR/APP. O perfil
// de vídeo WhatsApp usa bit 4 separado do FMT; PLI e FIR miram nosso SSRC.
func ParseRTCP(data []byte, localVideo uint32) ([]RTCPInfo, bool) {
	var out []RTCPInfo
	for len(data) > 0 {
		if len(data) < 4 || data[0]>>6 != 2 || data[1] < 192 || data[1] > 223 {
			return nil, false
		}
		n := (int(binary.BigEndian.Uint16(data[2:4])) + 1) * 4
		if n < 8 || n > len(data) {
			return nil, false
		}
		packet := data[:n]
		if packet[0]&0x20 != 0 {
			padding := int(packet[n-1])
			if n != len(data) || padding == 0 || padding > n-8 {
				return nil, false
			}
			packet = packet[:n-padding]
		}
		info := RTCPInfo{Type: packet[1], Format: packet[0] & 0x0f}
		if info.Type == 206 {
			if len(packet) < 12 {
				return nil, false
			}
			target := binary.BigEndian.Uint32(packet[8:12])
			switch info.Format {
			case 1:
				if len(packet) != 12 {
					return nil, false
				}
				info.RequestsKeyframe = localVideo != 0 && target == localVideo
			case 4:
				if len(packet) < 20 || (len(packet)-12)%8 != 0 {
					return nil, false
				}
				for fci := packet[12:]; len(fci) >= 8; fci = fci[8:] {
					if localVideo != 0 && binary.BigEndian.Uint32(fci[:4]) == localVideo {
						info.RequestsKeyframe = true
					}
				}
			}
		}
		out = append(out, info)
		data = data[n:]
	}
	return out, len(out) > 0
}
