package video

import (
	"testing"
	"wacalls/internal/voip/media"
)

func TestParseVideoOrientation(t *testing.T) {
	for orientation := 0; orientation < 4; orientation++ {
		for _, frameNumber := range []*uint16{nil, new(uint16(0x1020))} {
			ext := buildWhatsappVideoExt(videoMediaFrameIDR|byte(orientation), frameNumber, 5)
			h := &media.RtpHeader{Extension: true, ExtensionProfile: videoExtProfile, ExtensionData: ext}
			if got, ok := parseVideoOrientation(h); !ok || got != orientation {
				t.Fatalf("o=%d got=%d ok=%v ext=%x", orientation, got, ok, ext)
			}
		}
	}
	for _, tc := range []struct {
		name   string
		header *media.RtpHeader
	}{
		{"nil", nil},
		{"absent", &media.RtpHeader{}},
		{"wrong profile", &media.RtpHeader{Extension: true, ExtensionProfile: 0xBEDE, ExtensionData: []byte{0x30, 3, 0, 0}}},
		{"extension flag absent", &media.RtpHeader{ExtensionProfile: videoExtProfile, ExtensionData: []byte{0x30, 3, 0, 0}}},
		{"empty", &media.RtpHeader{Extension: true, ExtensionProfile: videoExtProfile}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, ok := parseVideoOrientation(tc.header); ok || got != 0 {
				t.Fatalf("got=%d ok=%v", got, ok)
			}
		})
	}
	for _, tc := range []struct {
		name string
		ext  []byte
	}{
		{"padding only", []byte{0, 0, 0, 0}},
		{"other element", []byte{0x51, 0, 0, 0}},
		{"missing value", []byte{0x30}},
		{"truncated word", []byte{0x32, 3}},
		{"truncated element", []byte{0x34, 3, 0, 0}},
		{"truncated other element", []byte{0x54, 0, 0, 0}},
		{"reserved ID", []byte{0xf0, 0, 0, 0}},
		{"nonzero padding ID", []byte{1, 0, 0, 0}},
		{"duplicate MediaFrameInfo", []byte{0x30, 1, 0x30, 2}},
		{"malformed after MediaFrameInfo", []byte{0x30, 3, 0xf0, 0}},
		{"truncated after MediaFrameInfo", []byte{0x30, 3, 0x52, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &media.RtpHeader{Extension: true, ExtensionProfile: videoExtProfile, ExtensionData: tc.ext}
			if got, ok := parseVideoOrientation(h); ok || got != 0 {
				t.Fatalf("got=%d ok=%v ext=%x", got, ok, tc.ext)
			}
		})
	}
	// Padding e outros elementos antes/depois do id3.
	h := &media.RtpHeader{Extension: true, ExtensionProfile: videoExtProfile, ExtensionData: []byte{0, 0x51, 0, 0, 0x30, 0xff, 0, 0}}
	if got, ok := parseVideoOrientation(h); !ok || got != 3 {
		t.Fatalf("got=%d ok=%v", got, ok)
	}
	// Como no upstream, qualquer comprimento que caiba no bloco pode conter
	// MediaFrameInfo: CVO fica sempre no primeiro byte, extras são ignorados.
	for size := 1; size <= 16; size++ {
		ext := append([]byte{0x30 | byte(size-1), 0x23}, make([]byte, size-1)...)
		for len(ext)%4 != 0 {
			ext = append(ext, 0)
		}
		h.ExtensionData = ext
		if got, ok := parseVideoOrientation(h); !ok || got != 3 {
			t.Fatalf("len=%d got=%d ok=%v", size, got, ok)
		}
	}
}

// O primeiro pacote de um access unit leva FrameNumber (id3 len3 = 0x32) e os
// blocos id5/id6/id9; o total é padded a múltiplo de 4 (16 bytes p/ este caso).
func TestVideoExtPrimeiroPacote(t *testing.T) {
	fn := uint16(1)
	ext := buildWhatsappVideoExt(videoMediaFrameIDR, &fn, 5)
	if len(ext)%4 != 0 {
		t.Fatalf("extensão precisa ser múltiplo de 4 bytes, veio %d", len(ext))
	}
	want := []byte{
		0x32, 0x08, 0x00, 0x01, // id3 len3: MediaFrameInfo=IDR, FrameNumber=1
		0x51, 0x00, 0x00, // id5 InitialBandwidth=0
		0x61, 0x00, 0x00, // id6 ShortOffset=0
		0x91, 0x00, 0x05, // id9 TransportSequence=5
		0x00, 0x00, 0x00, // padding até 16
	}
	if len(ext) != len(want) {
		t.Fatalf("tamanho = %d, quer %d (%x)", len(ext), len(want), ext)
	}
	for i := range want {
		if ext[i] != want[i] {
			t.Fatalf("byte %d = 0x%02x, quer 0x%02x (ext=%x)", i, ext[i], want[i], ext)
		}
	}
}

// Pacotes seguintes do mesmo access unit NÃO levam FrameNumber (id3 len1 = 0x30).
func TestVideoExtPacoteSeguinte(t *testing.T) {
	ext := buildWhatsappVideoExt(videoMediaFrameDelta, nil, 9)
	if len(ext)%4 != 0 {
		t.Fatalf("extensão precisa ser múltiplo de 4, veio %d", len(ext))
	}
	if ext[0] != 0x30 || ext[1] != videoMediaFrameDelta {
		t.Fatalf("cabeçalho id3 errado: %x", ext[:2])
	}
	// id9 (TransportSequence) deve estar presente com o valor certo.
	if ext[8] != 0x91 || ext[9] != 0x00 || ext[10] != 0x09 {
		t.Fatalf("bloco id9 errado: %x", ext)
	}
}
