package video

import (
	"encoding/hex"
	"time"

	"wacalls/internal/voip/media"
)

const videoDiagnosticInterval = 5 * time.Second
const videoReportInterval = 1500 * time.Millisecond

// Totais por chamada, preservados no rekey. Diferenças entre amostras de 5 s
// revelam a etapa que parou, inclusive se nenhum pacote novo chegar.
type videoDiagnostics struct {
	rxPackets, rxAuthRejected, rxAUs, rxComplete, rxForwarded, rxIDRs   uint64
	rxDrops                                                             map[string]uint64
	txAUs, txSubmitted, txBytes, browserKF, rtcpRejected, rtcpMalformed uint64
	txDrops                                                             map[string]uint64
	lastPacket, lastFrame, lastCapture                                  time.Time
	rembSeen, rembVideo                                                 bool
	rembBitrate, rembVideoBitrate, rembCount                            uint64
	rembAt                                                              time.Time
	rembSSRCs                                                           []uint32
	rembEncoderBitrate                                                  uint64
	srPackets, srOctets, srTimestamp                                    uint32
	srAt                                                                time.Time
	srRtpAt                                                             time.Time
	srCount, srErrors                                                   uint64
	lastLog                                                             time.Time
	lastReason                                                          string
	pliSent, pliErrors                                                  uint64
	rxBrowserKF, rxDeltaWhileWaiting                                    uint64
	rxSsrcChanges                                                       uint64
	rxAcceptedSamples, rxRejectedSamples                                uint8
	txDeltaSamples, txIDRSamples                                        uint8
}

// Orçamento fixo por chamada: até 6 aceitos + 16 rejeitados, mesmo após rekey.
// Somente RTP autenticado chega aqui. O hex é do payload original, antes do
// padding RTP, limitado a 24 bytes; nenhum frame completo é registrado.
func (p *Pipeline) logReceivePayloadLocked(pkt *media.RtpPacket, reason string, fuActive bool, fuHeader byte) {
	result, count, limit := "accepted", &p.diag.rxAcceptedSamples, uint8(6)
	if reason != "" {
		result, count, limit = "rejected", &p.diag.rxRejectedSamples, 16
	}
	if *count >= limit {
		return
	}
	*count++
	h, payload := pkt.Header, pkt.Payload
	kind, fuType := -1, -1
	start, end, reserved := false, false, false
	if len(payload) > 0 {
		kind = int(payload[0] & 31)
	}
	if kind == 28 && len(payload) >= 2 {
		fuType = int(payload[1] & 31)
		start, end, reserved = payload[1]&0x80 != 0, payload[1]&0x40 != 0, payload[1]&0x20 != 0
	}
	prefix := payload[:min(len(payload), 24)]
	p.log.Info("video receive H264 payload sample", "call_id", p.callID,
		"result", result, "sample", *count, "reason", reason,
		"pt", h.PayloadType, "marker", h.Marker, "seq", h.SequenceNumber, "ts", h.Timestamp, "ssrc", h.Ssrc,
		"payload_bytes", len(payload), "payload_hex", hex.EncodeToString(prefix), "rtp_padding", h.Padding,
		"nal_type", kind, "fu_type", fuType, "fu_start", start, "fu_end", end, "fu_reserved", reserved,
		"fu_active_before", fuActive, "fu_header_before", fuHeader)
}

func (p *Pipeline) logCapturedH264Locked(idr bool, auBytes, packedBytes, packets int, timestamp, step uint32) {
	count, limit := &p.diag.txDeltaSamples, uint8(4)
	if idr {
		count, limit = &p.diag.txIDRSamples, 8
	}
	if *count >= limit {
		return
	}
	*count++
	profile, constraints, level := -1, -1, -1
	if len(p.sendSPS) >= 4 {
		profile, constraints, level = int(p.sendSPS[1]), int(p.sendSPS[2]), int(p.sendSPS[3])
	}
	p.log.Info("video transmit H264 sample", "call_id", p.callID, "sample", *count, "idr", idr,
		"au_bytes", auBytes, "packed_bytes", packedBytes, "packets", packets, "ts", timestamp, "ts_step", step,
		"sps_bytes", len(p.sendSPS), "pps_bytes", len(p.sendPPS),
		"profile_idc", profile, "profile_iop", constraints, "level_idc", level)
}

func countReason(counts *map[string]uint64, reason string) {
	if *counts == nil {
		*counts = make(map[string]uint64)
	}
	(*counts)[reason]++
}

func ageMillis(now, at time.Time) int64 {
	if at.IsZero() {
		return -1
	}
	return now.Sub(at).Milliseconds()
}

func (p *Pipeline) startMonitorLocked() {
	if p.monitorTimer != nil || p.callID == "" || p.srtcp == nil {
		return
	}
	now := p.now()
	if p.diag.lastLog.IsZero() {
		p.diag.lastLog = now
	}
	delay := p.diag.lastLog.Add(videoDiagnosticInterval).Sub(now)
	if p.diag.srPackets > 0 {
		if p.diag.srAt.IsZero() {
			p.diag.srAt = now
		}
		if srDelay := p.diag.srAt.Add(videoReportInterval).Sub(now); srDelay < delay {
			delay = srDelay
		}
	}
	epoch := p.monitorEpoch
	p.monitorTimer = p.afterFunc(delay, func() { p.monitorVideo(epoch) })
}

func (p *Pipeline) stopMonitorLocked() {
	if p.monitorTimer != nil {
		p.monitorTimer.Stop()
		p.monitorTimer = nil
	}
	p.monitorEpoch++
}

func (p *Pipeline) monitorVideo(epoch uint64) {
	// Snapshot do transporte fora de p.mu: sender.step adquire p.mu em
	// valid(), sob sender.mu. Revalida o epoch depois do snapshot.
	p.mu.Lock()
	if epoch != p.monitorEpoch || p.srtcp == nil {
		p.mu.Unlock()
		return
	}
	reportPackets, reportOctets := p.diag.srPackets, p.diag.srOctets
	relay, ssrc := p.relay, p.selfSsrc
	p.mu.Unlock()
	if stats, ok := relay.(interface{ VideoSenderStats(uint32) (uint32, uint32) }); ok {
		reportPackets, reportOctets = stats.VideoSenderStats(ssrc)
	}
	p.mu.Lock()
	if epoch != p.monitorEpoch || p.srtcp == nil {
		p.mu.Unlock()
		return
	}
	p.monitorTimer = nil
	now := p.now()
	p.sendVideoReportLocked(now, reportPackets, reportOctets)
	logStats := now.Sub(p.diag.lastLog) >= videoDiagnosticInterval
	callID, cb := p.callID, p.OnDiagnostics
	if logStats {
		p.logDiagnosticsLocked(now)
	}
	p.startMonitorLocked()
	p.mu.Unlock()
	// sender.step consulta valid() sob sender.mu -> p.mu: nunca pedir o
	// snapshot dos senders enquanto segura p.mu.
	if logStats {
		if d, ok := relay.(interface{ LogVideoDiagnostics(string) }); ok {
			d.LogVideoDiagnostics(callID)
		}
		if cb != nil {
			cb(callID)
		}
	}
}

func (p *Pipeline) logDiagnosticsLocked(now time.Time) {
	d, r := &p.diag, &p.receive
	d.lastLog = now
	p.log.Info("video diagnostics", "call_id", p.callID,
		"rx_packets", d.rxPackets, "rx_auth_rejected", d.rxAuthRejected,
		"rx_aus", d.rxAUs, "rx_complete", d.rxComplete, "rx_forwarded", d.rxForwarded,
		"rx_idrs", d.rxIDRs, "rx_drops", d.rxDrops, "rx_last_reason", d.lastReason,
		"rx_browser_kf", d.rxBrowserKF, "rx_delta_while_waiting", d.rxDeltaWhileWaiting,
		"rx_ssrc_changes", d.rxSsrcChanges, "rx_sps_bytes", len(p.receiveSPS), "rx_pps_bytes", len(p.receivePPS),
		"rx_active", r.active, "rx_damaged", r.damaged, "rx_seq", r.lastSeq, "rx_timestamp", r.timestamp,
		"need_idr", r.needIDR, "idr_wait_ms", ageMillis(now, r.idrWaitAt),
		"peer_ssrc", r.ssrc, "self_ssrc", p.selfSsrc, "pli_attempts", p.peerPLICount, "pli_sent", d.pliSent, "pli_errors", d.pliErrors,
		"rx_last_packet_ms", ageMillis(now, d.lastPacket), "rx_last_frame_ms", ageMillis(now, d.lastFrame),
		"reorder_pending", len(p.reorder.pending), "reorder_bytes", p.reorder.bytes,
		"duplicates", p.reorder.duplicates, "reordered", p.reorder.reordered,
		"late", p.reorder.late, "missing", p.reorder.missing,
		"envelope", p.envelopeCapable, "frame_callback", p.OnFrame != nil,
		"tx_aus", d.txAUs, "tx_submitted", d.txSubmitted, "tx_bytes", d.txBytes, "tx_drops", d.txDrops,
		"tx_last_capture_ms", ageMillis(now, d.lastCapture), "tx_need_idr", p.keyframeRequired,
		"browser_kf_requests", d.browserKF, "sctp_buffered", p.relay.BufferedAmount(),
		"rtcp_rejected", d.rtcpRejected, "rtcp_malformed", d.rtcpMalformed,
		"remb_seen", d.rembSeen, "remb_bps", d.rembBitrate, "remb_for_video", d.rembVideo,
		"remb_video_bps", d.rembVideoBitrate, "remb_count", d.rembCount, "remb_age_ms", ageMillis(now, d.rembAt),
		"remb_ssrcs", d.rembSSRCs, "remb_encoder_bps", d.rembEncoderBitrate,
		"sr_sent", d.srCount, "sr_errors", d.srErrors)
}

func (p *Pipeline) sendVideoReportLocked(now time.Time, packets, octets uint32) {
	d := &p.diag
	if d.srPackets == 0 || now.Sub(d.srAt) < videoReportInterval {
		return
	}
	d.srAt = now
	if packets == 0 {
		return
	}
	if !p.relay.HasConnection() {
		return
	}
	// Timestamp do último RTP admitido + avanço do clock 90 kHz até o NTP
	// deste relatório. Não assume fps fixo do browser.
	elapsed := now.Sub(d.srRtpAt)
	timestamp := d.srTimestamp + uint32(elapsed/time.Second)*90000 + uint32(elapsed%time.Second*90000/time.Second)
	packet, err := p.srtcp.Protect(media.BuildVideoSenderReport(p.selfSsrc, timestamp, packets, octets, now.UnixMilli()))
	if err != nil {
		d.srErrors++
		return
	}
	if p.broadcastRTCPLocked(packet) == 0 {
		d.srErrors++
		return
	}
	d.srCount++
	if d.srCount == 1 {
		p.log.Info("video sender reports started", "call_id", p.callID, "self_ssrc", p.selfSsrc, "interval_ms", videoReportInterval.Milliseconds())
	}
}
