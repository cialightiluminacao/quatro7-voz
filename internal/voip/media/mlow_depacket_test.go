package media

import (
	"bytes"
	"testing"
)

func TestSplitContainer(t *testing.T) {
	// Container 0x92 válido: count=2, 1º sub-frame de 3 bytes, 2º = resto.
	p := []byte{0x92, 0x02, 0x03, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE}
	subs, ok := splitContainer(p)
	if !ok {
		t.Fatalf("container válido rejeitado")
	}
	if len(subs) != 2 {
		t.Fatalf("subs = %d, quer 2", len(subs))
	}
	if !bytes.Equal(subs[0], []byte{0xAA, 0xBB, 0xCC}) {
		t.Errorf("sub[0] = %x", subs[0])
	}
	if !bytes.Equal(subs[1], []byte{0xDD, 0xEE}) {
		t.Errorf("sub[1] = %x", subs[1])
	}

	// Container de 3 frames.
	p3 := []byte{0x92, 0x03, 0x02, 0x11, 0x22, 0x01, 0x33, 0x44, 0x55}
	subs3, ok := splitContainer(p3)
	if !ok || len(subs3) != 3 {
		t.Fatalf("container de 3: ok=%v len=%d", ok, len(subs3))
	}
	if !bytes.Equal(subs3[0], []byte{0x11, 0x22}) || !bytes.Equal(subs3[1], []byte{0x33}) || !bytes.Equal(subs3[2], []byte{0x44, 0x55}) {
		t.Errorf("subs3 = %x %x %x", subs3[0], subs3[1], subs3[2])
	}
}

func TestSplitContainerRejects(t *testing.T) {
	cases := map[string][]byte{
		"frame nu (não 0x92)":   {0x04, 0xAA, 0xBB},
		"frame nu SID":          {0x80, 0xAA, 0xBB, 0xCC},
		"curto demais":          {0x92, 0x02},
		"count inválido (1)":    {0x92, 0x01, 0xAA, 0xBB},
		"count inválido (9)":    {0x92, 0x09, 0x01, 0xAA},
		"len estoura o payload": {0x92, 0x02, 0xFF, 0xAA, 0xBB},
		"sem bytes pro último":  {0x92, 0x02, 0x02, 0xAA, 0xBB},
	}
	for name, p := range cases {
		if _, ok := splitContainer(p); ok {
			t.Errorf("%s: deveria rejeitar, aceitou", name)
		}
	}
}
