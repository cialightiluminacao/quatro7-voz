package media

import (
	"encoding/binary"
	"errors"
	"time"
)

const MaxWAVBytes = 30 << 20
const MaxWAVDuration = 300 * time.Second

// WAVPCM is uncompressed, little-endian PCM, mono, 16 bits at 16 or 48 kHz.
type WAVPCM struct {
	Samples []float32
	Rate    int
}

func (w WAVPCM) Duration() time.Duration {
	return time.Duration(len(w.Samples)) * time.Second / time.Duration(w.Rate)
}

func (w WAVPCM) AtRate(rate int) ([]float32, error) {
	switch {
	case rate == w.Rate:
		return w.Samples, nil
	case rate == 16000 && w.Rate == 48000:
		// Preserve the final partial group, which capture normally receives in full frames.
		samples := w.Samples
		if n := len(samples) % 3; n != 0 {
			samples = append(append([]float32(nil), samples...), make([]float32, 3-n)...)
		}
		return Downsample48to16(samples), nil
	case rate == 48000 && w.Rate == 16000:
		return Upsample16to48(w.Samples), nil
	default:
		return nil, errors.New("unsupported PCM sample rate")
	}
}

func DecodeWAV(b []byte) (WAVPCM, error) {
	bad := errors.New("expected WAV PCM 16-bit mono at 16000 or 48000 Hz")
	if len(b) > MaxWAVBytes {
		return WAVPCM{}, errors.New("WAV exceeds 30 MiB")
	}
	if len(b) < 12 || string(b[:4]) != "RIFF" || string(b[8:12]) != "WAVE" || uint64(binary.LittleEndian.Uint32(b[4:8]))+8 != uint64(len(b)) {
		return WAVPCM{}, bad
	}
	var rate int
	var data []byte
	fmtSeen, dataSeen := false, false
	for off := 12; off < len(b); {
		if len(b)-off < 8 {
			return WAVPCM{}, bad
		}
		name := string(b[off : off+4])
		n := uint64(binary.LittleEndian.Uint32(b[off+4 : off+8]))
		start := off + 8
		if n > uint64(len(b)-start) || n+(n&1) > uint64(len(b)-start) {
			return WAVPCM{}, bad
		}
		chunk := b[start : start+int(n)]
		switch name {
		case "fmt ":
			if fmtSeen || len(chunk) < 16 {
				return WAVPCM{}, bad
			}
			fmtSeen = true
			rate = int(binary.LittleEndian.Uint32(chunk[4:8]))
			if binary.LittleEndian.Uint16(chunk[:2]) != 1 || binary.LittleEndian.Uint16(chunk[2:4]) != 1 || (rate != 16000 && rate != 48000) || binary.LittleEndian.Uint16(chunk[14:16]) != 16 || binary.LittleEndian.Uint16(chunk[12:14]) != 2 || binary.LittleEndian.Uint32(chunk[8:12]) != uint32(rate*2) {
				return WAVPCM{}, bad
			}
		case "data":
			if dataSeen || len(chunk) == 0 || len(chunk)%2 != 0 {
				return WAVPCM{}, bad
			}
			dataSeen, data = true, chunk
		}
		off = start + int(n+(n&1))
	}
	if !fmtSeen || !dataSeen || len(data)/2 > rate*int(MaxWAVDuration/time.Second) {
		return WAVPCM{}, bad
	}
	pcm := make([]float32, len(data)/2)
	for i := range pcm {
		pcm[i] = float32(int16(binary.LittleEndian.Uint16(data[i*2:]))) / 32768
	}
	return WAVPCM{Samples: pcm, Rate: rate}, nil
}
