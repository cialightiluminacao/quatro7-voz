package main

import "wacalls/internal/voip/media"

// decodeBrowserAudio serializa os callbacks WebRTC/WS com o playout e o Close.
// Um callback da ponte substituída não pode usar o codec antigo, mesmo após swap.
func (s *Session) decodeBrowserAudio(callID string, ac *activeCall, codec media.Codec, payload []byte) []float32 {
	pcm, _ := s.decodeBrowserAudioWithEpoch(callID, ac, codec, payload)
	return pcm
}

func (s *Session) decodeBrowserAudioWithEpoch(callID string, ac *activeCall, codec media.Codec, payload []byte) ([]float32, uint64) {
	ac.mediaMu.Lock()
	defer ac.mediaMu.Unlock()
	_, current, ok := s.reg.mediaOf(callID)
	if !ok || codec == nil || current != codec {
		return nil, 0
	}
	pcm, err := codec.Decode(payload)
	if err != nil {
		return nil, 0
	}
	return pcm, ac.mediaEpoch.Load()
}

func (s *Session) feedBrowserAudio(callID string, ac *activeCall, codec media.Codec, payload []byte) {
	pcm, epoch := s.decodeBrowserAudioWithEpoch(callID, ac, codec, payload)
	if len(pcm) == 0 {
		return
	}
	// Não adquirir cm.mu sob mediaMu (OnStateChange pode fechar esta ponte).
	down := media.Downsample48to16(pcm)
	ac.recorder.writeBrowser(down)
	ac.cm.FeedBrowserPCM(epoch, down)
}
