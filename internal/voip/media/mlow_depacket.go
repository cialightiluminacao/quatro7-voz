package media

// Portado de 1bafdb1 (AstraCalls), originalmente de meowcaller (MIT, Rajeh Taher).
// splitContainer separa um container multi-frame 0x92 em seus sub-frames sequenciais:
//
//	0x92 <count> [ <len> <frame> ]*(count-1) <último frame = resto>
//
// Devolve ok=false pra qualquer coisa que não seja um container bem-formado (aí o
// chamador decodifica o payload como frame nu). O marcador 0x92 é distinto dos bytes
// de TOC de um frame nu, e a validação exige que os tamanhos fechem com o payload.
func splitContainer(p []byte) ([][]byte, bool) {
	if len(p) < 3 || p[0] != 0x92 {
		return nil, false
	}
	count := int(p[1])
	if count < 2 || count > 8 {
		return nil, false
	}
	frames := make([][]byte, 0, count)
	off := 2
	for i := 0; i < count-1; i++ {
		if off >= len(p) {
			return nil, false
		}
		flen := int(p[off])
		off++
		if off+flen > len(p) {
			return nil, false
		}
		frames = append(frames, p[off:off+flen])
		off += flen
	}
	if off >= len(p) {
		return nil, false
	}
	frames = append(frames, p[off:])
	return frames, true
}
