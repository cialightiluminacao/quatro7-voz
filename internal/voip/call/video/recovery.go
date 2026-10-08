package video

import (
	"time"
	"wacalls/internal/voip/media"
)

func (p *Pipeline) lockReceive() func() {
	if p.ReceiveGuard != nil {
		p.ReceiveGuard.Lock()
	}
	p.receiveMu.Lock()
	return func() {
		p.receiveMu.Unlock()
		if p.ReceiveGuard != nil {
			p.ReceiveGuard.Unlock()
		}
	}
}

// Requer mu e receiveMu. Callbacks fora de mu, serializados com os relays e
// timers; epoch impede continuar um lote após Reset/Setup durante o callback.
func (p *Pipeline) drainReceiveLocked(now time.Time) {
	epoch := p.receiveEpoch
	for {
		pkt, ok := p.reorder.pop(now)
		if !ok {
			return
		}
		frame, changed := p.receivePacketLocked(pkt)
		cb, orientationCB := p.OnFrame, p.OnOrientation
		callID, orientation := p.callID, p.peerOrientation
		p.mu.Unlock()
		if changed && orientationCB != nil {
			orientationCB(callID, orientation)
		}
		if frame != nil && cb != nil {
			cb(frame)
		}
		p.mu.Lock()
		if p.receiveEpoch != epoch {
			return
		}
	}
}

func (p *Pipeline) stopReceiveTimerLocked() {
	if p.receiveTimer != nil {
		p.receiveTimer.Stop()
		p.receiveTimer = nil
	}
	p.receiveEpoch++
}

func (p *Pipeline) scheduleReceiveLocked() {
	p.stopReceiveTimerLocked()
	if p.srtcp == nil {
		return
	}
	var deadline time.Time
	if len(p.reorder.pending) > 0 {
		deadline = p.reorder.deadline()
	}
	if p.receive.needIDR && p.receive.seqKnown && p.relay.HasConnection() {
		retry := p.lastPeerKFAt.Add(time.Second)
		if p.lastPeerKFAt.IsZero() {
			retry = p.now().Add(time.Second)
		}
		if deadline.IsZero() || retry.Before(deadline) {
			deadline = retry
		}
	}
	if deadline.IsZero() {
		return
	}
	epoch := p.receiveEpoch
	p.receiveTimer = time.AfterFunc(deadline.Sub(p.now()), func() { p.flushReceive(epoch) })
}

func (p *Pipeline) flushReceive(epoch uint64) {
	unlock := p.lockReceive()
	defer unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if epoch != p.receiveEpoch || p.srtp == nil {
		return
	}
	p.drainReceiveLocked(p.now())
	p.sendPeerKeyframeLocked()
	p.scheduleReceiveLocked()
}

// Formato de chamada individual: meowcaller c48c3e2 engine_media.go:1014-1026,
// 1395-1404 e rtp/rtcp.go:375-388. NÃO restaura o PLI 0x81 de ddd0c67,
// revertido por 9f18bd1; o perfil de vídeo WhatsApp inclui bit 4 (0x91).
func (p *Pipeline) sendPeerKeyframeLocked() {
	if !p.receive.needIDR || !p.receive.seqKnown || p.srtcp == nil || !p.relay.HasConnection() {
		return
	}
	now := p.now()
	if !p.lastPeerKFAt.IsZero() && now.Sub(p.lastPeerKFAt) < time.Second {
		return
	}
	packet, err := p.srtcp.Protect(media.BuildVideoPLI(p.selfSsrc, p.receive.ssrc))
	if err != nil {
		p.log.Debug("video keyframe request protection failed", "err", err)
		return
	}
	p.lastPeerKFAt = now
	p.relay.Broadcast(packet)
	p.log.Debug("video peer keyframe requested", "peer_ssrc", p.receive.ssrc)
}

// KF do browser pede IDR do peer. Pode chegar antes do primeiro RTP autenticado;
// nesse caso o pedido fica pendente até conhecer o SSRC real.
func (p *Pipeline) RequestPeerKeyframe() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.srtcp == nil {
		return
	}
	p.requireReceiveIDRLocked()
	p.sendPeerKeyframeLocked()
	p.scheduleReceiveLocked()
}

func (p *Pipeline) HandleRTCP(data []byte) {
	unlock := p.lockReceive()
	defer unlock()
	p.mu.Lock()
	if p.srtcp == nil || !media.IsRTCPPacket(data) {
		p.mu.Unlock()
		return
	}
	plain, err := p.srtcp.Unprotect(data)
	if err != nil {
		p.mu.Unlock()
		p.log.Debug("peer srtcp rejected", "err", err)
		return
	}
	infos, ok := media.ParseRTCP(plain, p.selfSsrc)
	if !ok {
		p.mu.Unlock()
		return
	}
	var request func()
	for _, info := range infos {
		if p.rtcpSeen == nil {
			p.rtcpSeen = make(map[uint16]bool)
		}
		kind := uint16(info.Type)<<8 | uint16(info.Format)
		if !p.rtcpSeen[kind] {
			p.rtcpSeen[kind] = true
			p.log.Info("peer RTCP authenticated", "pt", info.Type, "fmt", info.Format, "requests_keyframe", info.RequestsKeyframe)
		}
		if info.RequestsKeyframe {
			p.keyframeRequired = true
			if p.idrWaitAt.IsZero() {
				p.idrWaitAt = p.now()
			}
			if cb := p.requestKeyframeLocked(p.now()); cb != nil {
				request = cb
			}
		}
	}
	p.mu.Unlock()
	if request != nil {
		request()
	}
}
