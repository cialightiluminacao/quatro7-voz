package video

import (
	"bytes"
	"time"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

const maxReceiveAUBytes = 4 << 20

// MediaFrameInfo usa one-byte headers, apesar do profile específico 0xDEBE.
// Portado de AstraCalls 69e1ef8 pipeline.go:55-78; valida também o restante do
// bloco, para não aceitar orientação de uma extensão truncada/malformada.
func parseVideoOrientation(h *media.RtpHeader) (int, bool) {
	if h == nil || !h.Extension || h.ExtensionProfile != videoExtProfile || len(h.ExtensionData)%4 != 0 {
		return 0, false
	}
	ext := h.ExtensionData
	orientation, found := 0, false
	for off := 0; off < len(ext); {
		hdr := ext[off]
		off++
		if hdr == 0 {
			continue
		}
		id, size := hdr>>4, int(hdr&0x0f)+1
		if id == 0 || id == 15 || off+size > len(ext) {
			return 0, false
		}
		if id == 3 {
			if found {
				return 0, false
			}
			// Campos adicionais do MediaFrameInfo não alteram os 2 bits de CVO.
			orientation, found = int(ext[off]&3), true
		}
		off += size
	}
	return orientation, found
}

type receiveState struct {
	ssrc           uint32
	seqKnown       bool
	lastSeq        uint16
	timestamp      uint32
	haveTimestamp  bool
	active         bool
	damaged        bool
	needIDR        bool
	idrWaitAt      time.Time
	idrWaitExpired bool
	fuActive       bool
	fuHeader       byte
	orientation    int
	hasOrientation bool
	auSequence     uint32
	auBytes        int
}

func (p *Pipeline) discardReceiveAULocked(reason string) {
	if !p.receive.damaged {
		countReason(&p.diag.rxDrops, reason)
		p.diag.lastReason = reason
		p.log.Debug("video receive AU discarded", "reason", reason, "ssrc", p.receive.ssrc, "seq", p.receive.lastSeq, "timestamp", p.receive.timestamp,
			"duplicates", p.reorder.duplicates, "reordered", p.reorder.reordered, "late", p.reorder.late, "missing", p.reorder.missing)
	}
	p.frameBuf = nil
	p.depack = &transport.H264Depacketizer{}
	p.receive.damaged = true
	p.requireReceiveIDRLocked()
	p.receive.fuActive = false
}

// Requer mu: SRTP, sequência e remontagem ficam serializados entre os relays.
func (p *Pipeline) receivePacketLocked(pkt *media.RtpPacket) ([]byte, bool) {
	r, h := &p.receive, pkt.Header
	if r.seqKnown && r.ssrc != h.Ssrc {
		p.diag.rxSsrcChanges++
		if r.active {
			p.discardReceiveAULocked("ssrc-switch")
		}
		// Outro stream não pode completar o FU-A do anterior.
		*r = receiveState{auSequence: r.auSequence, needIDR: true, idrWaitAt: p.now()}
		p.receiveSPS, p.receivePPS = nil, nil
		p.frameBuf = nil
		p.depack = &transport.H264Depacketizer{}
	}
	if r.seqKnown {
		distance := int16(h.SequenceNumber - r.lastSeq)
		if distance <= 0 {
			return nil, false // duplicatas/atrasados nunca danificam a AU
		}
		if distance != 1 {
			p.diag.lastReason = "sequence-gap"
			p.log.Debug("video receive sequence gap", "missing", uint16(distance-1), "duplicates", p.reorder.duplicates, "reordered", p.reorder.reordered, "late", p.reorder.late)
			p.requireReceiveIDRLocked()
			if r.active && h.Timestamp == r.timestamp {
				p.discardReceiveAULocked("sequence-gap")
			}
		}
	}
	r.ssrc, r.lastSeq, r.seqKnown = h.Ssrc, h.SequenceNumber, true
	if r.needIDR && r.idrWaitAt.IsZero() {
		r.idrWaitAt = p.now()
	}
	if !r.haveTimestamp || h.Timestamp != r.timestamp {
		if r.active {
			p.discardReceiveAULocked("timestamp-before-marker") // marker/fim do FU-A perdido
		}
		p.frameBuf = nil
		p.depack = &transport.H264Depacketizer{}
		r.timestamp, r.haveTimestamp, r.active = h.Timestamp, true, true
		r.damaged, r.fuActive = false, false
		r.auBytes = 0
		r.orientation, r.hasOrientation = p.peerOrientation, false
		r.auSequence++
		p.diag.rxAUs++
	} else if !r.active {
		return nil, false // pacote depois do marker da mesma AU
	}
	if orientation, ok := parseVideoOrientation(h); ok && !r.hasOrientation {
		r.orientation, r.hasOrientation = orientation, true
	}
	if !r.damaged {
		r.auBytes += len(pkt.Payload)
		payload := pkt.Payload
		fuActive, fuHeader := r.fuActive, r.fuHeader
		paddingValid := true
		if h.Padding {
			paddingValid = len(payload) > 0 && payload[len(payload)-1] != 0 && int(payload[len(payload)-1]) <= len(payload)
			if paddingValid {
				payload = payload[:len(payload)-int(payload[len(payload)-1])]
			}
		}
		if r.auBytes > maxReceiveAUBytes {
			p.logReceivePayloadLocked(pkt, "access-unit-limit", fuActive, fuHeader)
			p.discardReceiveAULocked("access-unit-limit")
		} else if !paddingValid {
			p.logReceivePayloadLocked(pkt, "invalid-rtp-padding", fuActive, fuHeader)
			p.discardReceiveAULocked("invalid-rtp-padding")
		} else if !r.validPayload(payload) {
			p.logReceivePayloadLocked(pkt, "invalid-h264-payload", fuActive, fuHeader)
			p.discardReceiveAULocked("invalid-h264-payload")
		} else {
			p.logReceivePayloadLocked(pkt, "", fuActive, fuHeader)
			for _, nalu := range p.depack.Depacketize(payload) {
				if len(p.frameBuf)+len(nalu)+4 > maxReceiveAUBytes {
					p.discardReceiveAULocked("access-unit-limit")
					break
				}
				// O meowcaller também reconstrói FU-A cujo tipo original é 0
				// ou privado. Se o corpo já é Annex-B, o header sintetizado
				// não é uma NAL: preservar só o stream, sem inserir tipo 0/30.
				kind := nalu[0] & 31
				if (kind == 0 || kind > 23) && (bytes.HasPrefix(nalu[1:], annexBStartCode) || bytes.HasPrefix(nalu[1:], []byte{0, 0, 1})) {
					p.frameBuf = append(p.frameBuf, nalu[1:]...)
				} else {
					p.frameBuf = append(p.frameBuf, annexBStartCode...)
					p.frameBuf = append(p.frameBuf, nalu...)
				}
			}
		}
	}
	if !h.Marker {
		return nil, false
	}
	if r.fuActive {
		p.discardReceiveAULocked("incomplete-fu-at-marker") // marker com FU-A ainda incompleto
	}
	r.active = false
	changed := false
	if r.hasOrientation {
		changed = p.peerOrientation != r.orientation
		p.peerOrientation, p.rtpOrientationKnown = r.orientation, true
	}
	au := p.frameBuf
	p.frameBuf = nil
	if r.damaged || len(au) == 0 {
		if !r.damaged {
			countReason(&p.diag.rxDrops, "empty-au")
		}
		return nil, changed
	}
	p.diag.rxComplete++
	// WhatsApp pode fragmentar vários NALUs Annex-B como um único FU-A.
	nalus := transport.SplitAnnexB(au)
	idr := hasIDR(nalus)
	haveSPS, havePPS := false, false
	for _, n := range nalus {
		if len(n) == 0 {
			continue
		}
		switch n[0] & 31 {
		case 7:
			p.receiveSPS = append([]byte(nil), n...)
			haveSPS = true
		case 8:
			p.receivePPS = append([]byte(nil), n...)
			havePPS = true
		}
	}
	if r.needIDR && r.idrWaitAt.IsZero() {
		r.idrWaitAt = p.now()
	}
	if r.needIDR && idr {
		var prefix []byte
		if !haveSPS && len(p.receiveSPS) > 0 {
			prefix = append(prefix, annexBStartCode...)
			prefix = append(prefix, p.receiveSPS...)
		}
		if !havePPS && len(p.receivePPS) > 0 {
			prefix = append(prefix, annexBStartCode...)
			prefix = append(prefix, p.receivePPS...)
		}
		au = append(prefix, au...)
	}
	if r.needIDR && !idr {
		if p.now().Sub(r.idrWaitAt) < 2*time.Second {
			countReason(&p.diag.rxDrops, "waiting-idr")
			return nil, changed
		}
		if !r.idrWaitExpired {
			p.log.Info("video receive IDR wait expired", "call_id", p.callID, "peer_ssrc", r.ssrc, "idr_wait_ms", p.now().Sub(r.idrWaitAt).Milliseconds())
			r.idrWaitExpired = true
		}
		// AU completa continua delta. Mantém pedidos até IDR real; expirar a
		// espera não restaura referências de um decoder reiniciado no painel.
		p.diag.rxDeltaWhileWaiting++
	}
	if idr {
		p.diag.rxIDRs++
		if r.needIDR {
			p.log.Info("video receive IDR recovered", "call_id", p.callID, "peer_ssrc", r.ssrc, "idr_wait_ms", p.now().Sub(r.idrWaitAt).Milliseconds())
		}
		r.needIDR = false
		r.idrWaitAt = time.Time{}
		r.idrWaitExpired = false
	}
	if p.envelopeCapable {
		au = envelopeAU(au, r.orientation, idr, r.auSequence, h.Timestamp)
	}
	return au, changed
}

// Detecta continuação órfã, início/fim perdido, troca de NAL e STAP-A truncado.
func (r *receiveState) validPayload(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	switch kind := payload[0] & 0x1f; {
	case kind >= 1 && kind <= 23:
		return !r.fuActive
	case kind == 24:
		if r.fuActive || len(payload) < 4 {
			return false
		}
		haveNAL := false
		for body := payload[1:]; len(body) > 0; {
			// A referência devolve as NALs já completas quando encontra
			// um tail de zeros. Aceitar esse padding sem aceitar uma NAL
			// truncada ou um agregado vazio.
			if haveNAL && len(bytes.Trim(body, "\x00")) == 0 {
				return true
			}
			if len(body) < 2 {
				return false
			}
			size := int(body[0])<<8 | int(body[1])
			body = body[2:]
			if size == 0 || size > len(body) {
				return false
			}
			body = body[size:]
			haveNAL = true
		}
		return true
	case kind == 28:
		if len(payload) < 2 {
			return false
		}
		start, end, typ := payload[1]&0x80 != 0, payload[1]&0x40 != 0, payload[1]&0x1f
		// Compatibilidade com meowcaller rtp/h264.go: S+E numa FU pequena,
		// fragmentos vazios e todos os 5 bits de tipo original. R é ignorado
		// pelo receptor conforme RFC 6184 §5.8. Sequência, header e limites
		// continuam estritos; não se adivinha nem remove prefixo proprietário.
		// iPhone/WhatsApp (chamada real 08/10): só o 1º fragmento traz NRI; os seguintes vêm com
		// NRI=0 (7c87… → 1c07…). Comparar F/NRI rejeitava toda continuação — SPS/IDR nunca fechavam.
		// O fragmento é amarrado só pelo tipo original; o cabeçalho do NAL vem do 1º fragmento.
		header := typ
		if start {
			if r.fuActive {
				return false
			}
			r.fuActive, r.fuHeader = true, header
		} else if !r.fuActive || r.fuHeader != header {
			return false
		}
		if end {
			r.fuActive = false
		}
		return true
	default:
		return false
	}
}

func (p *Pipeline) requireReceiveIDRLocked() {
	if !p.receive.needIDR || p.receive.idrWaitAt.IsZero() {
		// KF pode chegar antes de o peer ligar a câmera. A espera medida
		// começa com RTP conhecido, não com a fase de áudio da chamada.
		if p.receive.seqKnown {
			p.receive.idrWaitAt = p.now()
		}
		p.receive.idrWaitExpired = false
	}
	p.receive.needIDR = true
}
