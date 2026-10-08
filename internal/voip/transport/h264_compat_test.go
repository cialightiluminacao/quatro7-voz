package transport

import (
	"bytes"
	"testing"
)

func TestH264ReferenceFUAVariants(t *testing.T) {
	// A referência usa todos os 5 bits de tipo original e ignora R.
	for typ := byte(0); typ < 32; typ++ {
		for _, reserved := range []byte{0, 0x20} {
			var d H264Depacketizer
			payload := []byte{0x7c, 0xc0 | reserved | typ, 1, 2}
			got := d.Depacketize(payload)
			if len(got) != 1 || !bytes.Equal(got[0], []byte{0x60 | typ, 1, 2}) {
				t.Fatalf("S+E type=%d R=%x got=%x", typ, reserved, got)
			}
			if got := d.Depacketize([]byte{0x7c, 0x80 | reserved | typ}); len(got) != 0 {
				t.Fatal(got)
			}
			if got := d.Depacketize([]byte{0x7c, reserved | typ, 1, 2}); len(got) != 0 {
				t.Fatal(got)
			}
			got = d.Depacketize([]byte{0x7c, 0x40 | reserved | typ})
			if len(got) != 1 || !bytes.Equal(got[0], []byte{0x60 | typ, 1, 2}) {
				t.Fatalf("empty-fragment type=%d R=%x got=%x", typ, reserved, got)
			}
		}
	}
}

func TestH264DepacketizerRejectsMalformedAndUnsupportedPayloads(t *testing.T) {
	for _, payload := range [][]byte{
		nil, {0}, {0x7c}, {0x7c, 0x45, 1},
		{0x78, 0, 2, 0x65}, {0x78, 0, 0},
		{0x78, 0, 2, 0x65, 1, 1}, {0x78, 0, 2, 0x65, 1, 0, 9},
		{0x79, 0, 0, 0, 2, 0x65, 1}, {0x7a, 0, 0}, {0x7b, 0, 0}, {0x7d, 0xc5, 0, 0, 1}, {0x7e}, {0x7f},
	} {
		var d H264Depacketizer
		if got := d.Depacketize(payload); len(got) != 0 {
			t.Fatalf("payload=%x got=%x", payload, got)
		}
	}
	for _, last := range [][]byte{{0x7c, 0x48, 2}, {0x5c, 0x45, 2}} {
		var d H264Depacketizer
		d.Depacketize([]byte{0x7c, 0x85, 1})
		if got := d.Depacketize(last); len(got) != 0 || d.fuActive {
			t.Fatal("FU com tipo/NRI diferente aceito")
		}
	}
}

func TestH264ReassemblyLimitIncludesFirstFragment(t *testing.T) {
	var d H264Depacketizer
	oversized := append([]byte{0x7c, 0xc5}, bytes.Repeat([]byte{1}, maxFuReassemblyBytes)...)
	if got := d.Depacketize(oversized); len(got) != 0 || d.fuActive {
		t.Fatal("primeiro FU ultrapassou limite")
	}
	start := oversized[:len(oversized)-1]
	start[1] = 0x85
	d.Depacketize(start)
	if !d.fuActive {
		t.Fatal("FU no limite foi rejeitado")
	}
	if got := d.Depacketize([]byte{0x7c, 0x45, 2}); len(got) != 0 || d.fuActive {
		t.Fatal("continuação ultrapassou limite")
	}
	if got := d.Depacketize([]byte{0x7c, 0xc5, 2}); len(got) != 1 || !bytes.Equal(got[0], []byte{0x65, 2}) {
		t.Fatal("overflow impediu recuperação")
	}
}

func TestH264PacketizerMTUBoundariesAndMarkers(t *testing.T) {
	for _, size := range []int{1, 799, 800, 801, 1597, 1598, 1599} {
		nalu := append([]byte{0x65}, bytes.Repeat([]byte{9}, size-1)...)
		packets := PackageH264NALU(nalu)
		for i, payload := range packets {
			if len(payload) > 800 {
				t.Fatalf("size=%d packet=%d len=%d", size, i, len(payload))
			}
			if size > 800 {
				if payload[0] != 0x7c || payload[1]&31 != 5 || (payload[1]&0x80 != 0) != (i == 0) || (payload[1]&0x40 != 0) != (i == len(packets)-1) || payload[1]&0xc0 == 0xc0 {
					t.Fatalf("size=%d packet=%d FU header=%x", size, i, payload[:2])
				}
			}
		}
		var d H264Depacketizer
		got := depacketizeAll(&d, packets)
		if len(got) != 1 || !bytes.Equal(got[0], nalu) {
			t.Fatalf("size=%d roundtrip=%x", size, got)
		}
	}
}
