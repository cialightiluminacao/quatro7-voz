package video

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"testing"
	"time"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

type capturedVideoJob struct {
	idr   bool
	at    time.Time
	valid func() bool
	drop  func()
}
type scheduledRelay struct {
	*fakeRelay
	jobs []capturedVideoJob
}

func (r *scheduledRelay) BroadcastVideoAU(packets [][]byte, idr bool, at time.Time, valid func() bool, drop func()) {
	r.jobs = append(r.jobs, capturedVideoJob{idr, at, valid, drop})
}

func TestCapturedStaleAgeIncludesLocksAndRequestsKFWithoutQ1(t *testing.T) {
	p, base, _ := testPipeline(t)
	r := &scheduledRelay{fakeRelay: base}
	p.relay = r
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	requests := 0
	p.OnKeyframeRequired = func() { requests++; p.mu.Lock(); p.mu.Unlock() }
	idr, delta := []byte{0, 0, 0, 1, 0x65, 1}, []byte{0, 0, 0, 1, 0x41, 2}
	p.FeedCapturedAt(idr, now.Add(-transport.VideoMaxAge))
	if len(r.jobs) != 0 || requests != 1 {
		t.Fatal("AU velha enviada ou não pediu KF")
	}
	p.FeedCapturedAt(idr, now.Add(-transport.VideoMaxAge+time.Nanosecond))
	if len(r.jobs) != 1 || !r.jobs[0].idr || !r.jobs[0].valid() {
		t.Fatal("IDR fresca rejeitada")
	}
	now = now.Add(time.Second)
	r.jobs[0].drop()
	p.FeedCaptured(delta)
	if requests != 2 || len(r.jobs) != 1 {
		t.Fatal("drop não aguardou novo IDR")
	}
	p.FeedCaptured(idr)
	if len(r.jobs) != 2 {
		t.Fatal("IDR de recuperação rejeitada")
	}
	p.DisableEnvelope()
	if r.jobs[1].valid() {
		t.Fatal("bridge substituída não cancelou job")
	}
	now = now.Add(time.Second)
	r.jobs[1].drop()
	if requests != 2 {
		t.Fatal("job cancelado pediu KF na nova bridge")
	}
}

func TestFirstPLIHexCountSurvivesRekeyAndResetsPerCall(t *testing.T) {
	p, _, peer := testPipeline(t)
	var buf bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	now := time.Unix(100, 0)
	manualVideoClock(p, &now)
	receivePacket(t, p, peer, 1, 1, true, []byte{0x65, 1}, nil)
	p.RequestPeerKeyframe()
	now = now.Add(time.Second)
	p.RequestPeerKeyframe()
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := p.Setup("CID", "999:1@lid", "111:1@lid", km, km); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	receivePacket(t, p, peer, 2, 2, true, []byte{0x41, 2}, nil)
	first, attempts := 0, 0
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte{'\n'}) {
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			t.Fatal(err)
		}
		if fields["msg"] == "video first PLI" {
			first++
			if fields["count"] != float64(1) || fields["srtcp_index"] != float64(1) || fields["compound_hex"] != hex.EncodeToString(media.BuildVideoKeyframeRequest(p.selfSsrc, 42)) || fields["srtcp_hex"] == "" {
				t.Fatal(fields)
			}
		}
		if fields["msg"] == "video peer keyframe requested" {
			attempts++
		}
	}
	if first != 1 || attempts != 3 || p.peerPLICount != 3 {
		t.Fatalf("first=%d attempts=%d count=%d", first, attempts, p.peerPLICount)
	}
	p.Reset()
	if p.peerPLICount != 0 {
		t.Fatal("contador vazou para outra call")
	}
}

func TestDelayedRelayDropDoesNotInvalidateNewerCapturedIDR(t *testing.T) {
	p, base, _ := testPipeline(t)
	r := &scheduledRelay{fakeRelay: base}
	p.relay = r
	requests := 0
	p.OnKeyframeRequired = func() { requests++ }
	idr, delta := []byte{0, 0, 0, 1, 0x65, 1}, []byte{0, 0, 0, 1, 0x41, 2}
	p.FeedCaptured(idr)
	p.FeedCaptured(delta)
	p.FeedCaptured(idr)
	r.jobs[1].drop() // relay atrasado sinaliza perda anterior ao IDR novo
	p.FeedCaptured(delta)
	if requests != 1 || len(r.jobs) != 4 || p.keyframeRequired {
		t.Fatal("IDR mais novo invalidado por drop antigo")
	}
}
