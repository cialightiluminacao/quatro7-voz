package signaling

import (
	"context"
	"strings"
	"testing"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/wanode"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

type acceptDestinationSocket struct {
	core.VoipSocket
	asserted  []types.JID
	encrypted []types.JID
}

// WACRG SIG-03 and the meowcaller accept builder agree on the relay medium
// and ordered audio/net/encopt body. The media key has already arrived in offer.
func TestAcceptRelayFormat(t *testing.T) {
	peer := types.NewJID("222", types.HiddenUserServer)
	sock := &acceptDestinationSocket{}
	node, err := BuildAcceptStanza(context.Background(), sock, "test-call", make([]byte, 32), peer, peer, false)
	if err != nil {
		t.Fatal(err)
	}
	accept := findChild(&node, "accept")
	if accept == nil {
		t.Fatal("accept missing")
	}
	net := findChild(accept, "net")
	if net == nil || wanode.AttrString(net.Attrs, "medium") != "2" {
		t.Error("accept must select relay medium 2; medium 3 belongs to offer")
	}
	var tags []string
	for _, child := range wanode.NodeChildren(accept) {
		tags = append(tags, child.Tag)
	}
	if got := strings.Join(tags, ","); got != "audio,net,encopt" {
		t.Errorf("accept body = %s; expected audio,net,encopt", got)
	}
}

func (s *acceptDestinationSocket) AssertSessions(_ context.Context, jids []types.JID, _ bool) error {
	s.asserted = append([]types.JID(nil), jids...)
	return nil
}

func (s *acceptDestinationSocket) CreateParticipantNodes(_ context.Context, jids []types.JID, _ []byte, attrs waBinary.Attrs) ([]waBinary.Node, bool, error) {
	s.encrypted = append([]types.JID(nil), jids...)
	return []waBinary.Node{{Tag: "enc", Attrs: attrs, Content: []byte("test-ciphertext")}}, false, nil
}

// An incoming offer identifies the actual calling device. Answering a companion
// must not redirect the accept to its primary account, even when both accounts
// happen to be hosted by the same motor.
func TestAcceptPreservesCallingDevice(t *testing.T) {
	for _, jid := range []string{"111@s.whatsapp.net", "111:7@s.whatsapp.net", "222@lid", "222:9@lid"} {
		t.Run(jid, func(t *testing.T) {
			peer, err := types.ParseJID(jid)
			if err != nil {
				t.Fatal(err)
			}
			sock := &acceptDestinationSocket{}
			node, err := BuildAcceptStanza(context.Background(), sock, "test-call", make([]byte, 32), peer, peer, false)
			if err != nil {
				t.Fatal(err)
			}
			if got := wanode.AttrString(node.Attrs, "to"); got != jid {
				t.Fatalf("accept destination = %q; calling device = %q", got, jid)
			}
			if len(sock.asserted) != 0 || len(sock.encrypted) != 0 {
				t.Fatal("accept must not re-encrypt the key already delivered in offer")
			}
			accept := findChild(&node, "accept")
			if accept == nil || wanode.AttrString(accept.Attrs, "call-creator") != jid {
				t.Fatal("original call creator must be preserved")
			}
		})
	}
}
