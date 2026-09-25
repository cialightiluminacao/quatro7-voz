package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVideoAcceptComCamera(t *testing.T) {
	casos := map[string]bool{
		"":                     true, // sem corpo = comportamento antigo
		`{}`:                   true,
		`{"withCamera":true}`:  true,
		`{"withCamera":false}`: false,
		`lixo`:                 true,
	}
	for corpo, want := range casos {
		r := httptest.NewRequest("POST", "/x", strings.NewReader(corpo))
		if got := videoAcceptComCamera(r); got != want {
			t.Errorf("corpo %q: got %v, want %v", corpo, got, want)
		}
	}
}
