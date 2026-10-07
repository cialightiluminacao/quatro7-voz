//go:build mlow

package media

import (
	"math"
	"slices"
	"sync"
	"testing"
)

func TestMLowCodecRoundtrip(t *testing.T) {
	codec, err := NewMLowCodec(DefaultCodecOptions)
	if err != nil {
		t.Fatalf("NewMLowCodec: %v", err)
	}
	defer codec.Close()

	if codec.FrameSize() != 960 || codec.SampleRate() != 16000 {
		t.Fatalf("unexpected frame=%d rate=%d", codec.FrameSize(), codec.SampleRate())
	}

	frame := make([]float32, 960)
	for i := range frame {
		frame[i] = 0.3 * float32(math.Sin(2*math.Pi*440*float64(i)/16000))
	}

	encoded, err := codec.Encode(frame)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("encoded frame is empty")
	}
	t.Logf("encoded %d samples → %d bytes (MLow)", len(frame), len(encoded))

	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(decoded) == 0 {
		t.Fatal("decoded PCM is empty")
	}
	t.Logf("decoded → %d samples", len(decoded))

	plc, err := codec.Decode(nil)
	if err != nil {
		t.Fatalf("Decode(nil) PLC: %v", err)
	}
	if len(plc) == 0 {
		t.Fatal("PLC returned no samples")
	}
}

func TestMLowContainerMatchesSequentialBareDecode(t *testing.T) {
	encoder, err := NewMLowCodec(DefaultCodecOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	bare, err := NewMLowCodec(DefaultCodecOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer bare.Close()
	wrapped, err := NewMLowCodec(DefaultCodecOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer wrapped.Close()
	var frames [][]byte
	var want []float32
	for i := range 3 {
		pcm := make([]float32, 960)
		for j := range pcm {
			pcm[j] = .2 * float32(math.Sin(2*math.Pi*float64(300+i*100)*float64(j)/16000))
		}
		frame, err := encoder.Encode(pcm)
		if err != nil || len(frame) == 0 || len(frame) > 255 {
			t.Fatalf("frame %d: bytes=%d err=%v", i, len(frame), err)
		}
		frames = append(frames, frame)
		decoded, err := bare.Decode(frame)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, decoded...)
	}
	container := []byte{0x92, byte(len(frames))}
	for i, frame := range frames {
		if i < len(frames)-1 {
			container = append(container, byte(len(frame)))
		}
		container = append(container, frame...)
	}
	got, err := wrapped.Decode(container)
	if err != nil || len(got) != 3*960 || !slices.Equal(got, want) {
		t.Fatalf("container não equivale aos frames sequenciais: len=%d want=%d err=%v", len(got), len(want), err)
	}
	// O estado deixado no decoder também deve ser idêntico para o próximo frame.
	next, _ := wrapped.Decode(frames[0])
	nextBare, _ := bare.Decode(frames[0])
	if !slices.Equal(next, nextBare) {
		t.Fatal("container deixou estado diferente no decoder")
	}
}

func TestOpusGenericConcurrentUseAndClose(t *testing.T) {
	codec, err := NewOpusCodec(48000, 960)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(make([]float32, 960))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			<-start
			for range 100 {
				codec.Encode(make([]float32, 960))
				codec.Decode(encoded)
			}
		})
	}
	wg.Go(func() { <-start; codec.Close() })
	close(start)
	wg.Wait()
	codec.Close() // Close idempotente, inclusive após callbacks antigos
	if _, err := codec.Decode(encoded); err == nil {
		t.Fatal("Decode após Close deveria devolver erro")
	}
	if _, err := codec.Encode(make([]float32, 960)); err == nil {
		t.Fatal("Encode após Close deveria devolver erro")
	}
}
