package main

import (
	"strconv"
	"sync/atomic"
	"time"
)

type bridgeVideoDiagnostics struct {
	captured, captureBytes, received, sent, sentBytes       atomic.Uint64
	noChannel, sendErrors, envelopeRequests, peerKFRequests atomic.Uint64
	kfSent, kfErrors, bitrateSent, bitrateErrors            atomic.Uint64
	maxSendUS                                               atomic.Int64
}

// B1 é opt-in independente de Q1: browsers antigos não recebem texto que
// poderiam passar ao decoder como mídia. Motor -> browser: "B1 <bits/s>".
// REMB não anuncia resolução; a política de resolução pertence ao encoder.
func (b *Bridge) SetVideoBitrate(bps uint64) {
	b.videoControlMu.Lock()
	defer b.videoControlMu.Unlock()
	b.targetBitrate, b.bitrateKnown = bps, true
	b.sendVideoBitrateLocked()
}

func (b *Bridge) sendVideoBitrateLocked() {
	if !b.bitrateCapable || !b.bitrateKnown {
		return
	}
	now := time.Now()
	if !b.lastBitrateAt.IsZero() && b.targetBitrate == b.lastBitrateSent && now.Sub(b.lastBitrateAt) < 5*time.Second {
		return
	}
	dc := b.videoDC.Load()
	if dc == nil {
		return
	}
	if err := dc.SendText("B1 " + strconv.FormatUint(b.targetBitrate, 10)); err != nil {
		b.videoStats.bitrateErrors.Add(1)
		return
	}
	b.lastBitrateAt, b.lastBitrateSent = now, b.targetBitrate
	b.videoStats.bitrateSent.Add(1)
}

func (b *Bridge) LogVideoDiagnostics() {
	dc := b.videoDC.Load()
	state, buffered := "missing", uint64(0)
	if dc != nil {
		state, buffered = dc.ReadyState().String(), dc.BufferedAmount()
	}
	b.videoControlMu.Lock()
	capable, known, target := b.bitrateCapable, b.bitrateKnown, b.targetBitrate
	b.sendVideoBitrateLocked() // refresh recupera perda no canal sem retransmissão
	b.videoControlMu.Unlock()
	d := &b.videoStats
	b.log.Info("video browser diagnostics", "call_id", b.callID, "dc_state", state, "dc_buffered", buffered,
		"captured_aus", d.captured.Load(), "captured_bytes", d.captureBytes.Load(),
		"rx_aus", d.received.Load(), "sent_aus", d.sent.Load(), "sent_bytes", d.sentBytes.Load(),
		"no_channel", d.noChannel.Load(), "send_errors", d.sendErrors.Load(), "send_max_us", d.maxSendUS.Swap(0),
		"q1_requests", d.envelopeRequests.Load(), "peer_kf_requests", d.peerKFRequests.Load(),
		"kf_sent", d.kfSent.Load(), "kf_errors", d.kfErrors.Load(),
		"bitrate_capable", capable, "bitrate_known", known, "target_bps", target,
		"bitrate_sent", d.bitrateSent.Load(), "bitrate_errors", d.bitrateErrors.Load())
}
