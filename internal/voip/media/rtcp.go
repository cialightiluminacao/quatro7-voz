package media

import (
	"encoding/binary"
	"fmt"
)

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

// Compound mínimo AVPF: RR sem report blocks, SDES/CNAME e feedback.
// Todos os headers mantêm o bit de perfil de vídeo WhatsApp (0x10), como
// meowcaller c48c3e2 rtp/rtcp.go:308-346,377-388 com profileExtension=true.
// Sem esse bit o header PLI seria o 0x81 do RTP/AVPF padrão.
func BuildVideoKeyframeRequest(sender, peer uint32) []byte {
	rr := []byte{0x90, 201, 0, 1, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(rr[4:8], sender)
	// Mesma forma nativa de 18 bytes (xxxxx@pjxxxxxx.org), estável por SSRC,
	// sem JID. Referência: meowcaller rtp/rtcp.go:287-320.
	cname := fmt.Sprintf("%05x@pj%06x.org", sender>>12, sender&0xffffff)
	sdes := []byte{0x91, 202, 0, 0, 0, 0, 0, 0, 1, byte(len(cname))}
	binary.BigEndian.PutUint32(sdes[4:8], sender)
	sdes = append(sdes, cname...)
	sdes = append(sdes, 0) // END item e alinhamento de 32 bits
	for len(sdes)%4 != 0 {
		sdes = append(sdes, 0)
	}
	binary.BigEndian.PutUint16(sdes[2:4], uint16(len(sdes)/4-1))
	return append(append(rr, sdes...), BuildVideoPLI(sender, peer)...)
}

type RTCPInfo struct {
	Type, Format     uint8
	RequestsKeyframe bool
	REMB             bool
	REMBBitrate      uint64
	REMBForVideo     bool
	REMBSSRCs        []uint32
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
			case 15:
				// AFB não implica REMB. Só interpreta o identificador e a lista
				// completos; audio REMB não pode limitar o encoder de vídeo.
				if len(packet) >= 16 && string(packet[12:16]) == "REMB" {
					if len(packet) < 20 || packet[16] == 0 || len(packet) != 20+4*int(packet[16]) {
						return nil, false
					}
					exponent := uint(packet[17] >> 2)
					mantissa := uint64(packet[17]&3)<<16 | uint64(packet[18])<<8 | uint64(packet[19])
					if mantissa > ^uint64(0)>>exponent {
						return nil, false
					}
					info.REMB, info.REMBBitrate = true, mantissa<<exponent
					for list := packet[20:]; len(list) > 0; list = list[4:] {
						ssrc := binary.BigEndian.Uint32(list[:4])
						info.REMBSSRCs = append(info.REMBSSRCs, ssrc)
						if localVideo != 0 && ssrc == localVideo {
							info.REMBForVideo = true
						}
					}
				}
			}
		}
		out = append(out, info)
		data = data[n:]
	}
	return out, len(out) > 0
}

// SR+SDES de vídeo, perfil WhatsApp, igual ao meowcaller rtp/rtcp.go:265-346.
// packets/octets contam RTP admitido ao transporte (sem duplicar relays).
func BuildVideoSenderReport(sender, timestamp, packets, octets uint32, nowMs int64) []byte {
	sr := make([]byte, 28)
	sr[0], sr[1], sr[3] = 0x90, 200, 6
	binary.BigEndian.PutUint32(sr[4:8], sender)
	binary.BigEndian.PutUint32(sr[8:12], uint32(nowMs/1000+2208988800))
	binary.BigEndian.PutUint32(sr[12:16], uint32(uint64(nowMs%1000)*(1<<32)/1000))
	binary.BigEndian.PutUint32(sr[16:20], timestamp)
	binary.BigEndian.PutUint32(sr[20:24], packets)
	binary.BigEndian.PutUint32(sr[24:28], octets)
	// Reusa o mesmo CNAME de RR+SDES+PLI, sem incluir RR ou PLI no SR.
	compound := BuildVideoKeyframeRequest(sender, 0)
	return append(sr, compound[8:len(compound)-12]...)
}
