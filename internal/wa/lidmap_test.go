package wa

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

type fakeMapper struct{ lids, pns []types.JID }

func (f *fakeMapper) StoreLIDPNMapping(_ context.Context, lid, pn types.JID) {
	f.lids = append(f.lids, lid)
	f.pns = append(f.pns, pn)
}

func TestLIDDaRespostaGravaParPNLID(t *testing.T) {
	pn := types.NewJID("5511999990000", types.DefaultUserServer)
	lid := types.NewJID("123456789", types.HiddenUserServer)
	resp := []types.IsOnWhatsAppResponse{{IsIn: true, JID: lid}}
	m := &fakeMapper{}
	got := lidDaResposta(context.Background(), m, pn, resp)
	if got != lid {
		t.Fatalf("lid = %v", got)
	}
	if len(m.lids) != 1 || m.lids[0] != lid || m.pns[0] != pn {
		t.Fatalf("par não gravado: %v %v", m.lids, m.pns)
	}
}

func TestLIDDaRespostaSemLIDNaoGrava(t *testing.T) {
	pn := types.NewJID("5511999990000", types.DefaultUserServer)
	m := &fakeMapper{}
	got := lidDaResposta(context.Background(), m, pn, []types.IsOnWhatsAppResponse{{IsIn: false, JID: pn}})
	if !got.IsEmpty() || len(m.lids) != 0 {
		t.Fatalf("não devia resolver/gravar: %v %v", got, m.lids)
	}
}
