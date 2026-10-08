package call

import (
	"context"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/signaling"
	"wacalls/internal/voip/wanode"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

// videoPeerLocked resolve o destino das stanzas de vídeo mid-call: o dispositivo
// que aceitou a chamada quando conhecido, senão o JID base do peer. Requer m.mu.
func (m *CallManager) videoPeerLocked() types.JID {
	if m.acceptedByJid != "" {
		return wanode.MustJID(m.acceptedByJid)
	}
	return wanode.MustJID(m.currentCall.PeerJid)
}

// VideoState devolve um snapshot do estado atual sob mu.
func (m *CallManager) VideoState() CallStateData {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentCall == nil {
		return CallStateData{}
	}
	return m.currentCall.StateData
}

// O callback recebe uma cópia: nenhuma leitura da UI disputa com a próxima ação.
// Requer mu; os callbacks de vídeo não devem chamar o CallManager.
func (m *CallManager) emitVideoStateLocked(motivo string) CallStateData {
	m.currentCall.StateData.VideoMotivo = motivo
	snapshot := m.currentCall.StateData
	if m.OnVideoStateChanged != nil {
		c := *m.currentCall
		m.OnVideoStateChanged(&c)
	}
	return snapshot
}

func videoCallActive(c *CallInfo) bool {
	return c != nil && (c.IsActive() || c.StateData.State == core.CallStateOnHold)
}

// notePeerVideoActive marca a câmera do peer quando chegam frames reais.
// Idempotente: só emite na transição off->on.
func (m *CallManager) notePeerVideoActive() {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.currentCall
	if !videoCallActive(c) || c.StateData.PeerVideoOn {
		return
	}
	c.StateData.PeerVideoOn = true
	c.MediaType = core.CallMediaTypeVideo
	m.emitVideoStateLocked("peer-enabled")
	m.log.Info("peer video ativo (frames recebidos)", "call_id", c.CallID)
}

func (m *CallManager) notePeerVideoOrientation(callID string, orientation int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.currentCall
	if c == nil || c.IsEnded() || c.CallID != callID {
		return
	}
	m.notePeerVideoOrientationLocked(orientation)
}

func (m *CallManager) notePeerVideoOrientationLocked(orientation int) {
	if orientation < 0 || orientation > 3 || m.currentCall.StateData.PeerOrientation == orientation {
		return
	}
	m.currentCall.StateData.PeerOrientation = orientation
	m.emitVideoStateLocked("peer-orientation")
}

// HandleVideoState confirma a stanza e aplica apenas a negociação ainda vigente.
func (m *CallManager) HandleVideoState(ctx context.Context, node *waBinary.Node) {
	info := signaling.ExtractNodeInfo(node)
	if info == nil || info.Tag != "video" {
		return
	}
	// Captura ANTES do ack (I/O): um cancelamento durante o envio invalida a resposta.
	m.mu.Lock()
	epoch, c := m.videoEpoch, m.currentCall
	m.mu.Unlock()
	if ack, ok := signaling.BuildVideoAck(node); ok {
		_ = m.sock.SendNode(ctx, ack)
	}
	state := wanode.AttrInt(info.InnerNode.Attrs, "state", -1)

	m.mu.Lock()
	defer m.mu.Unlock()
	if c == nil || c.IsEnded() || c != m.currentCall || c.CallID != info.CallID {
		return
	}
	// Orientação é independente do state/epoch da negociação: a stanza pode
	// conter só device_orientation. CVO RTP já conhecido tem precedência.
	if orientation := wanode.AttrInt(info.InnerNode.Attrs, "device_orientation", -1); orientation >= 0 && orientation <= 3 {
		m.notePeerVideoOrientationLocked(m.video.SetOrientationFallback(orientation))
	}
	if !videoCallActive(c) {
		return
	}
	if epoch != m.videoEpoch && (state == signaling.VideoStateUpgradeAccept || state == signaling.VideoStateUpgradeReject) {
		return
	}
	sd := &c.StateData
	previous := *c
	var motivo string
	var upgradeRequested, sendEnabled bool
	switch state {
	case signaling.VideoStateUpgradeRequest, signaling.VideoStateUpgradeRequestV2:
		sd.VideoUpgradeIncoming = true
		upgradeRequested = true
		motivo = "upgrade-requested"
	case signaling.VideoStateUpgradeAccept:
		// Um aceite tardio não liga novamente a câmera após stop/cancel.
		if !sd.VideoUpgradeOutgoing {
			return
		}
		sd.VideoUpgradeOutgoing = false
		sd.VideoOff = false
		c.MediaType = core.CallMediaTypeVideo
		sendEnabled = true
		motivo = "upgrade-accepted"
	case signaling.VideoStateUpgradeReject:
		sd.VideoUpgradeOutgoing = false
		motivo = "upgrade-rejected"
	case signaling.VideoStateUpgradeCancel:
		sd.VideoUpgradeIncoming = false
		motivo = "upgrade-cancelled"
	case signaling.VideoStateEnabled:
		sd.PeerVideoOn = true
		c.MediaType = core.CallMediaTypeVideo
		motivo = "peer-enabled"
		if sd.VideoUpgradeOutgoing && epoch == m.videoEpoch {
			sd.VideoUpgradeOutgoing = false
			sd.VideoOff = false
			sendEnabled = true
			motivo = "upgrade-accepted"
		}
	case signaling.VideoStateStopped, signaling.VideoStateDisabled:
		sd.PeerVideoOn = false
		motivo = "peer-stopped"
	default:
		return
	}
	if motivo == "upgrade-accepted" || motivo == "upgrade-rejected" {
		m.videoEpoch++
	}
	epoch = m.videoEpoch
	peer, creator := m.videoPeerLocked(), wanode.MustJID(c.CallCreator)
	if upgradeRequested && m.OnVideoUpgradeRequest != nil {
		c.StateData.VideoMotivo = motivo
		snapshot := *c
		m.OnVideoUpgradeRequest(&snapshot)
	}
	m.emitVideoStateLocked(motivo)
	m.log.Info("peer video state", "call_id", c.CallID, "state", state)
	if sendEnabled {
		// Não segura mu durante I/O; o epoch protege a confirmação e a falha.
		orientation := 0
		n := signaling.BuildVideoStateStanza(signaling.VideoStateParams{
			CallID: c.CallID, To: peer, CallCreator: creator,
			State: signaling.VideoStateEnabled, DeviceOrientation: &orientation,
		})
		m.mu.Unlock()
		_ = m.sendVideoStates(ctx, c, epoch, previous, n)
		m.mu.Lock()
	}
}

func (m *CallManager) videoActionLocked() (*CallInfo, CallInfo, error) {
	c := m.currentCall
	if !videoCallActive(c) {
		if c != nil {
			m.emitVideoStateLocked("falha")
		}
		return nil, CallInfo{}, &CallError{"no active call"}
	}
	m.videoEpoch++
	return c, *c, nil
}

// sendVideoStates não confirma nem desfaz uma ação que já foi substituída.
func (m *CallManager) sendVideoStates(ctx context.Context, c *CallInfo, epoch uint64, previous CallInfo, nodes ...waBinary.Node) error {
	m.videoSendMu.Lock()
	defer m.videoSendMu.Unlock()
	for _, n := range nodes {
		m.mu.Lock()
		current := m.currentCall == c && videoCallActive(c) && m.videoEpoch == epoch
		m.mu.Unlock()
		if !current {
			return nil
		}
		if err := m.sock.SendNode(ctx, n); err != nil {
			m.mu.Lock()
			if m.currentCall == c && videoCallActive(c) && m.videoEpoch == epoch {
				// Falha local não desfaz mute/hold nem o estado/orientação do peer.
				c.StateData.VideoOff = previous.StateData.VideoOff
				c.StateData.VideoUpgradeIncoming = previous.StateData.VideoUpgradeIncoming
				c.StateData.VideoUpgradeOutgoing = previous.StateData.VideoUpgradeOutgoing
				if !c.StateData.PeerVideoOn {
					c.MediaType = previous.MediaType
				}
				m.videoEpoch++
				m.emitState()
				m.emitVideoStateLocked("falha")
			}
			m.mu.Unlock()
			return err
		}
		for _, child := range n.GetChildren() {
			if child.Tag == "video" && wanode.AttrInt(child.Attrs, "state", -1) == signaling.VideoStateUpgradeAccept {
				// O peer já recebeu 4: falhar no 1 mantém o aceite, sem câmera.
				previous.StateData.VideoUpgradeIncoming = false
				previous.StateData.VideoOff = true
				previous.MediaType = core.CallMediaTypeVideo
			}
		}
	}
	return nil
}

// RequestVideoUpgrade liga imediatamente numa chamada já de vídeo; em áudio,
// manda state=11 e espera a aceitação do peer.
func (m *CallManager) RequestVideoUpgrade(ctx context.Context) (CallStateData, error) {
	m.mu.Lock()
	c, previous, err := m.videoActionLocked()
	if err != nil {
		m.mu.Unlock()
		return CallStateData{}, err
	}
	alreadyVideo := c.MediaType == core.CallMediaTypeVideo || c.StateData.PeerVideoOn
	state, dec, motivo := signaling.VideoStateUpgradeRequestV2, signaling.VideoDecRequest, "upgrade-requested"
	if alreadyVideo {
		c.MediaType = core.CallMediaTypeVideo
		c.StateData.VideoOff = false
		c.StateData.VideoUpgradeOutgoing = false
		state, dec, motivo = signaling.VideoStateEnabled, "", "local-enabled"
	} else {
		c.StateData.VideoUpgradeOutgoing = true
	}
	orientation := 0
	n := signaling.BuildVideoStateStanza(signaling.VideoStateParams{
		CallID: c.CallID, To: m.videoPeerLocked(), CallCreator: wanode.MustJID(c.CallCreator),
		State: state, Dec: dec, DeviceOrientation: &orientation,
	})
	epoch := m.videoEpoch
	m.emitState()
	snapshot := m.emitVideoStateLocked(motivo)
	m.mu.Unlock()
	return snapshot, m.sendVideoStates(ctx, c, epoch, previous, n)
}

func (m *CallManager) AcceptVideoUpgrade(ctx context.Context) error {
	_, err := m.AcceptVideoUpgradeWith(ctx, true)
	return err
}

// withCamera=false aceita somente a recepção, sem anunciar câmera nossa.
func (m *CallManager) AcceptVideoUpgradeWith(ctx context.Context, withCamera bool) (CallStateData, error) {
	m.mu.Lock()
	c, previous, err := m.videoActionLocked()
	if err != nil {
		m.mu.Unlock()
		return CallStateData{}, err
	}
	c.StateData.VideoUpgradeIncoming = false
	c.StateData.VideoOff = !withCamera
	c.MediaType = core.CallMediaTypeVideo
	params := signaling.VideoStateParams{
		CallID: c.CallID, To: m.videoPeerLocked(), CallCreator: wanode.MustJID(c.CallCreator),
		State: signaling.VideoStateUpgradeAccept, Dec: signaling.VideoDecAccept,
	}
	nodes := []waBinary.Node{signaling.BuildVideoStateStanza(params)}
	if withCamera {
		orientation := 0
		params.State, params.Dec, params.DeviceOrientation = signaling.VideoStateEnabled, "", &orientation
		nodes = append(nodes, signaling.BuildVideoStateStanza(params))
	}
	epoch := m.videoEpoch
	m.emitState()
	snapshot := m.emitVideoStateLocked("upgrade-accepted")
	m.mu.Unlock()
	return snapshot, m.sendVideoStates(ctx, c, epoch, previous, nodes...)
}

func (m *CallManager) RejectVideoUpgrade(ctx context.Context) (CallStateData, error) {
	m.mu.Lock()
	c, previous, err := m.videoActionLocked()
	if err != nil {
		m.mu.Unlock()
		return CallStateData{}, err
	}
	c.StateData.VideoUpgradeIncoming = false
	n := signaling.BuildVideoStateStanza(signaling.VideoStateParams{
		CallID: c.CallID, To: m.videoPeerLocked(), CallCreator: wanode.MustJID(c.CallCreator),
		State: signaling.VideoStateUpgradeReject,
	})
	epoch := m.videoEpoch
	m.emitState()
	snapshot := m.emitVideoStateLocked("upgrade-rejected")
	m.mu.Unlock()
	return snapshot, m.sendVideoStates(ctx, c, epoch, previous, n)
}

// StopVideo cancela um upgrade pendente (8), ou desliga a câmera ativa (6).
func (m *CallManager) StopVideo(ctx context.Context) (CallStateData, error) {
	m.mu.Lock()
	c, previous, err := m.videoActionLocked()
	if err != nil {
		m.mu.Unlock()
		return CallStateData{}, err
	}
	state, motivo := signaling.VideoStateStopped, "local-stopped"
	if c.StateData.VideoUpgradeOutgoing {
		state, motivo = signaling.VideoStateUpgradeCancel, "upgrade-cancelled"
	}
	c.StateData.VideoOff = true
	c.StateData.VideoUpgradeOutgoing = false
	orientation := 0
	n := signaling.BuildVideoStateStanza(signaling.VideoStateParams{
		CallID: c.CallID, To: m.videoPeerLocked(), CallCreator: wanode.MustJID(c.CallCreator),
		State: state, DeviceOrientation: &orientation,
	})
	epoch := m.videoEpoch
	m.emitState()
	snapshot := m.emitVideoStateLocked(motivo)
	m.mu.Unlock()
	return snapshot, m.sendVideoStates(ctx, c, epoch, previous, n)
}
