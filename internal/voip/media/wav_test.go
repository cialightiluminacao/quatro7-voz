package media

import (
	"encoding/binary"
	"slices"
	"testing"
	"time"
)

func wavFixture(rate, samples int) []byte {
	b := make([]byte, 44+2*samples)
	copy(b, "RIFF")
	binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-8))
	copy(b[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(b[16:], 16)
	binary.LittleEndian.PutUint16(b[20:], 1)
	binary.LittleEndian.PutUint16(b[22:], 1)
	binary.LittleEndian.PutUint32(b[24:], uint32(rate))
	binary.LittleEndian.PutUint32(b[28:], uint32(rate*2))
	binary.LittleEndian.PutUint16(b[32:], 2)
	binary.LittleEndian.PutUint16(b[34:], 16)
	copy(b[36:], "data")
	binary.LittleEndian.PutUint32(b[40:], uint32(samples*2))
	return b
}

func TestWAVValidAndResample(t *testing.T) {
	for _, rate := range []int{16000, 48000} {
		b := wavFixture(rate, rate/10)
		for i, v := range []uint16{0x8000, 0, 0x4000, 0x7fff} {
			binary.LittleEndian.PutUint16(b[44+i*2:], v)
		}
		w, err := DecodeWAV(b)
		if err != nil || w.Rate != rate || w.Duration() != 100*time.Millisecond || !slices.Equal(w.Samples[:4], []float32{-1, 0, .5, 32767.0 / 32768}) {
			t.Fatalf("rate=%d err=%v WAV=%+v", rate, err, w)
		}
		for _, target := range []int{16000, 48000} {
			pcm, err := w.AtRate(target)
			if err != nil || len(pcm) != target/10 {
				t.Fatalf("resample %d -> %d: len=%d err=%v", rate, target, len(pcm), err)
			}
		}
	}
	// Same algorithms as capture, including a trailing partial 48k group.
	w := WAVPCM{Samples: []float32{.3, .6, .9, .3}, Rate: 48000}
	pcm, err := w.AtRate(16000)
	if err != nil || len(pcm) != 2 || pcm[1] != .1 {
		t.Fatalf("partial group: %v %v", pcm, err)
	}
}

func TestWAVRejectsMalformedAndHuge(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func([]byte) []byte
	}{
		{"header", func(b []byte) []byte { b[0] = 'X'; return b }},
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }},
		{"compressed", func(b []byte) []byte { b[20] = 3; return b }},
		{"stereo", func(b []byte) []byte { b[22] = 2; return b }},
		{"8bit", func(b []byte) []byte { b[34] = 8; return b }},
		{"rate", func(b []byte) []byte { binary.LittleEndian.PutUint32(b[24:], 44100); return b }},
		{"byteRate", func(b []byte) []byte { b[28] = 1; return b }},
		{"alignment", func(b []byte) []byte { b[32] = 1; return b }},
		{"chunkOverflow", func(b []byte) []byte { binary.LittleEndian.PutUint32(b[40:], 0xffffffff); return b }},
		{"empty", func(b []byte) []byte { return wavFixture(16000, 0) }},
		{"oddData", func(b []byte) []byte { binary.LittleEndian.PutUint32(b[40:], 1); return b }},
		{"noFmt", func(b []byte) []byte { copy(b[12:], "JUNK"); return b }},
		{"noData", func(b []byte) []byte { copy(b[36:], "JUNK"); return b }},
		{"duration", func(b []byte) []byte { return wavFixture(16000, 16000*301) }},
		{"size", func(b []byte) []byte { return make([]byte, MaxWAVBytes+1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeWAV(tc.edit(wavFixture(16000, 4))); err == nil {
				t.Fatal("invalid WAV accepted")
			}
		})
	}
	// Optional odd-length metadata chunks must include their RIFF pad byte.
	b := wavFixture(16000, 4)
	junk := []byte{'J', 'U', 'N', 'K', 1, 0, 0, 0, 7, 0}
	b = append(append(append([]byte{}, b[:12]...), junk...), b[12:]...)
	binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-8))
	if _, err := DecodeWAV(b); err != nil {
		t.Fatal(err)
	}
}
