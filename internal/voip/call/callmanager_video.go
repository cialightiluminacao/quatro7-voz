package call

func (m *CallManager) FeedCapturedVideo(au []byte) {
	m.video.FeedCaptured(au)
}

// EnableVideoEnvelope é chamado somente pelo texto Q1 do DataChannel h264.
func (m *CallManager) EnableVideoEnvelope(callID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentCall != nil && m.currentCall.CallID == callID && !m.currentCall.IsEnded() {
		m.video.EnableEnvelope()
	}
}

func (m *CallManager) ResetVideoEnvelope() { m.video.DisableEnvelope() }
