package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

type playbackAudio struct {
	AudioBase64 string `json:"audioBase64"`
}

const maxPlaybackJSONBytes = (media.MaxWAVBytes+2)/3*4 + 4096

func readPlaybackJSON(w http.ResponseWriter, r *http.Request, body any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxPlaybackJSONBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(body); err != nil {
		return errors.New("invalid or oversized playback JSON")
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}

// Playback accepts only a base64 WAV supplied by the BFF; no remote downloads.
func (a playbackAudio) load() (media.WAVPCM, error) {
	if a.AudioBase64 == "" {
		return media.WAVPCM{}, errors.New("audioBase64 is required")
	}
	if base64.StdEncoding.DecodedLen(len(a.AudioBase64)) > media.MaxWAVBytes+2 {
		return media.WAVPCM{}, errors.New("WAV exceeds 30 MiB")
	}
	data, err := base64.StdEncoding.Strict().DecodeString(a.AudioBase64)
	if err != nil {
		return media.WAVPCM{}, errors.New("invalid audioBase64")
	}
	return media.DecodeWAV(data)
}

func (s *server) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	id := r.PathValue("id")
	ac, ok := sess.reg.get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	var body struct {
		playbackAudio
		HangupAfter bool `json:"hangupAfter"`
		MaxSeconds  *int `json:"maxSeconds"`
	}
	if err := readPlaybackJSON(w, r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	seconds := 120
	if body.MaxSeconds != nil {
		seconds = *body.MaxSeconds
	}
	if seconds < 1 || seconds > 300 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "maxSeconds must be an integer from 1 to 300"})
		return
	}
	wav, err := body.playbackAudio.load()
	if err != nil || (err == nil && wav.Duration() > time.Duration(seconds)*time.Second) {
		if err == nil {
			err = errors.New("audio duration exceeds maxSeconds")
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ac.bridgeOpMu.Lock()
	if record, exists := s.broker.getCall(sess.id, id); exists && record.Owner != nil {
		err = errors.New("announce requires a call without a human owner")
	} else {
		err = ac.cm.Announce(r.Context(), id, wav, body.HangupAfter, time.Duration(seconds)*time.Second)
	}
	ac.bridgeOpMu.Unlock()
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status": "playing", "call": map[string]string{"callId": id}, "hangupAfter": body.HangupAfter, "maxSeconds": seconds,
	})
}

func (s *server) handleHoldMusic(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	id := r.PathValue("id")
	ac, ok := sess.reg.get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	var body playbackAudio
	if err := readPlaybackJSON(w, r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	wav, err := body.load()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ac.bridgeOpMu.Lock()
	state := ac.cm.VideoState()
	if state.State != core.CallStateActive && state.State != core.CallStateOnHold {
		err = errors.New("hold-music requires an active call")
	} else {
		// A is detached without terminating the WhatsApp leg, allowing a private consultation.
		sess.swapBrowserMedia(id, ac, nil, nil, nil)
		err = ac.cm.HoldMusic(id, wav)
	}
	ac.bridgeOpMu.Unlock()
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "held": true, "moh": true})
}
