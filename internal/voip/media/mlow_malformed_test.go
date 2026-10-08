//go:build mlow

package media

import (
	"math/rand/v2"
	"testing"
)

func TestMLowMalformedPacketsDoNotCrash(t *testing.T) {
	codec, err := NewMLowCodec(DefaultCodecOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer codec.Close()
	rng := rand.New(rand.NewPCG(92, 20000))
	for i := range 20000 {
		packet := make([]byte, 1+i%1500)
		for j := range packet {
			packet[j] = byte(rng.Uint32())
		}
		if i%3 == 0 {
			packet[0] = 0x92
		}
		// Erros de decode são aceitáveis; abort() na biblioteca C não é.
		_, _ = codec.Decode(packet)
	}
}
