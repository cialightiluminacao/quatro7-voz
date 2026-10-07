package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func novaSessaoWebhook(url, secret string, events []string) *Session {
	s := &Session{id: "s1", log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.setWebhook(url, secret, events)
	return s
}

type chegada struct {
	body string
	sig  string
}

func servidorCaptura(t *testing.T) (*httptest.Server, chan chegada) {
	t.Helper()
	ch := make(chan chegada, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ch <- chegada{string(b), r.Header.Get("X-Webhook-Signature")}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, ch
}

func esperaUma(t *testing.T, ch chan chegada) chegada {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("webhook não entregue")
	}
	return chegada{}
}

func TestWebhookFiltroEntregaSoEventoDaLista(t *testing.T) {
	srv, ch := servidorCaptura(t)
	s := novaSessaoWebhook(srv.URL, "", []string{"recording"})
	s.dispatchWebhook("receipt", map[string]any{"x": 1}) // fora da lista
	s.dispatchWebhook("recording", map[string]any{"url": "u"})
	c := esperaUma(t, ch)
	if !strings.Contains(c.body, `"event":"recording"`) {
		t.Fatalf("esperava recording, veio %s", c.body)
	}
	select {
	case c := <-ch:
		t.Fatalf("evento fora da lista foi entregue: %s", c.body)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestWebhookListaVaziaEntregaTudo(t *testing.T) {
	srv, ch := servidorCaptura(t)
	s := novaSessaoWebhook(srv.URL, "", nil)
	s.dispatchWebhook("receipt", map[string]any{})
	s.dispatchWebhook("message", map[string]any{})
	esperaUma(t, ch)
	esperaUma(t, ch)
}

func TestWebhookAssinaHMACQuandoHaSecret(t *testing.T) {
	srv, ch := servidorCaptura(t)
	s := novaSessaoWebhook(srv.URL, "segredo", nil)
	s.dispatchWebhook("message", map[string]any{})
	c := esperaUma(t, ch)
	if want := assinaWebhook("segredo", []byte(c.body)); c.sig != want || !strings.HasPrefix(c.sig, "sha256=") {
		t.Fatalf("assinatura %q, esperado %q", c.sig, want)
	}
}

func TestWebhookViewNaoVazaSecret(t *testing.T) {
	s := novaSessaoWebhook("http://x", "segredo-super", []string{"recording"})
	v := webhookView(s)
	if v["hasSecret"] != true {
		t.Fatalf("hasSecret deveria ser true: %v", v)
	}
	for k, val := range v {
		if strings.Contains(strings.ToLower(k), "secret") && k != "hasSecret" {
			t.Fatalf("campo %q vaza secret", k)
		}
		if str, ok := val.(string); ok && strings.Contains(str, "segredo-super") {
			t.Fatalf("valor de %q contém o secret", k)
		}
	}
}

func TestSplitEvents(t *testing.T) {
	got := splitEvents(" recording, ,message ")
	if len(got) != 2 || got[0] != "recording" || got[1] != "message" {
		t.Fatalf("got %v", got)
	}
	if splitEvents("") != nil {
		t.Fatal("vazio deve dar nil")
	}
}
