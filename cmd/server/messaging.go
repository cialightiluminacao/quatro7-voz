package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

var mediaHTTP = &http.Client{Timeout: 30 * time.Second}

// pairedSession devolve a sessão se existir E estiver pareada, ou escreve o erro.
func (s *server) pairedSession(w http.ResponseWriter, sid string) *Session {
	sess := s.sessionByID(w, sid)
	if sess == nil {
		return nil
	}
	if sess.client.Store.ID == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not paired"})
		return nil
	}
	return sess
}

// resolveRecipient aceita um número (DDI+DDD+num) ou um JID completo (com @).
func resolveRecipient(to string) (types.JID, error) {
	to = strings.TrimSpace(to)
	if to == "" {
		return types.JID{}, errors.New("recipient required")
	}
	if strings.Contains(to, "@") {
		return types.ParseJID(to)
	}
	return types.NewJID(normalizePhone(to), types.DefaultUserServer), nil
}

// fetchMedia obtém os bytes da mídia a partir de base64 (data) ou de uma URL.
func fetchMedia(b64, url string) ([]byte, error) {
	if b64 != "" {
		if strings.HasPrefix(b64, "data:") {
			if i := strings.Index(b64, ","); i > 0 {
				b64 = b64[i+1:]
			}
		}
		return base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	}
	if url != "" {
		resp, err := mediaHTTP.Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, errors.New("download failed: " + resp.Status)
		}
		return io.ReadAll(io.LimitReader(resp.Body, 100<<20)) // teto de 100MB
	}
	return nil, errors.New("base64 or url required")
}

func (s *server) send(sess *Session, w http.ResponseWriter, r *http.Request, to string, msg *waE2E.Message) {
	jid, err := resolveRecipient(to)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.sendTo(sess, w, r, jid, msg)
}

// uploadFor faz o download/decode + upload da mídia, retornando a mensagem montada
// pela função builder. Centraliza o tratamento de erro de mídia.
func (s *server) uploadMedia(sess *Session, w http.ResponseWriter, r *http.Request, b64, url string, mt whatsmeow.MediaType) (*whatsmeow.UploadResponse, bool) {
	data, err := fetchMedia(b64, url)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return nil, false
	}
	up, err := sess.client.Upload(r.Context(), data, mt)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return nil, false
	}
	return &up, true
}

// ---- Handlers de envio ----

func (s *server) handleSendText(w http.ResponseWriter, r *http.Request) {
	sess := s.pairedSession(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var b struct {
		To   string `json:"to"`
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil || strings.TrimSpace(b.Text) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "to and text required"})
		return
	}
	s.send(sess, w, r, b.To, &waE2E.Message{Conversation: proto.String(b.Text)})
}

func (s *server) handleSendImage(w http.ResponseWriter, r *http.Request) {
	sess := s.pairedSession(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var b struct {
		To, Base64, URL, Caption, Mimetype string
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	up, ok := s.uploadMedia(sess, w, r, b.Base64, b.URL, whatsmeow.MediaImage)
	if !ok {
		return
	}
	mime := b.Mimetype
	if mime == "" {
		mime = "image/jpeg"
	}
	s.send(sess, w, r, b.To, &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Caption: proto.String(b.Caption), Mimetype: proto.String(mime),
		URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
		FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
	}})
}

func (s *server) handleSendAudio(w http.ResponseWriter, r *http.Request) {
	sess := s.pairedSession(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var b struct {
		To, Base64, URL, Mimetype string
		PTT                       bool
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	up, ok := s.uploadMedia(sess, w, r, b.Base64, b.URL, whatsmeow.MediaAudio)
	if !ok {
		return
	}
	mime := b.Mimetype
	if mime == "" {
		mime = "audio/ogg; codecs=opus"
	}
	s.send(sess, w, r, b.To, &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
		Mimetype: proto.String(mime), PTT: proto.Bool(b.PTT),
		URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
		FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
	}})
}

func (s *server) handleSendVideo(w http.ResponseWriter, r *http.Request) {
	sess := s.pairedSession(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var b struct {
		To, Base64, URL, Caption, Mimetype string
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	up, ok := s.uploadMedia(sess, w, r, b.Base64, b.URL, whatsmeow.MediaVideo)
	if !ok {
		return
	}
	mime := b.Mimetype
	if mime == "" {
		mime = "video/mp4"
	}
	s.send(sess, w, r, b.To, &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
		Caption: proto.String(b.Caption), Mimetype: proto.String(mime),
		URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
		FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
	}})
}

// documentWithCaption monta a mensagem de documento, embrulhando-a em
// documentWithCaptionMessage quando há legenda. É o formato que o WhatsApp oficial
// usa para exibir a legenda do arquivo; um documentMessage.Caption "solto" muitas
// vezes não aparece no destino. Sem legenda, envia o documentMessage puro.
func documentWithCaption(doc *waE2E.DocumentMessage, caption string) *waE2E.Message {
	if caption != "" {
		doc.Caption = proto.String(caption)
		return &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{DocumentMessage: doc},
		}}
	}
	return &waE2E.Message{DocumentMessage: doc}
}

func (s *server) handleSendDocument(w http.ResponseWriter, r *http.Request) {
	sess := s.pairedSession(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var b struct {
		To, Base64, URL, FileName, Mimetype, Caption string
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	up, ok := s.uploadMedia(sess, w, r, b.Base64, b.URL, whatsmeow.MediaDocument)
	if !ok {
		return
	}
	mime := b.Mimetype
	if mime == "" {
		mime = "application/octet-stream"
	}
	name := b.FileName
	if name == "" {
		name = "file"
	}
	s.send(sess, w, r, b.To, documentWithCaption(&waE2E.DocumentMessage{
		FileName: proto.String(name), Title: proto.String(name), Mimetype: proto.String(mime),
		URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
		FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
	}, b.Caption))
}

func (s *server) handleSendSticker(w http.ResponseWriter, r *http.Request) {
	sess := s.pairedSession(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var b struct {
		To, Base64, URL, Mimetype string
		Animated                  bool
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	// Figurinha precisa ser WebP pronto (512x512). Não convertemos aqui.
	up, ok := s.uploadMedia(sess, w, r, b.Base64, b.URL, whatsmeow.MediaImage)
	if !ok {
		return
	}
	mime := b.Mimetype
	if mime == "" {
		mime = "image/webp"
	}
	s.send(sess, w, r, b.To, &waE2E.Message{StickerMessage: &waE2E.StickerMessage{
		Mimetype: proto.String(mime), IsAnimated: proto.Bool(b.Animated),
		URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
		FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
	}})
}

// ---- Handlers de configuração do webhook ----

func (s *server) handleSetWebhook(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var b struct {
		URL    string   `json:"url"`
		Secret string   `json:"secret"` // opcional: assina o payload (X-Webhook-Signature: sha256=…)
		Events []string `json:"events"` // opcional: só entrega estes tipos; vazio = todos
	}
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "url required"})
		return
	}
	url := strings.TrimSpace(b.URL)
	secret := strings.TrimSpace(b.Secret)
	events := make([]string, 0, len(b.Events))
	for _, e := range b.Events {
		if e = strings.TrimSpace(e); e != "" {
			events = append(events, e)
		}
	}
	sess.setWebhook(url, secret, events)
	_ = sess.mgr.store.setWebhook(r.Context(), sess.id, url, secret, strings.Join(events, ","))
	writeJSON(w, http.StatusOK, webhookView(sess))
}

// splitEvents divide um CSV de tipos de evento em lista, ignorando vazios/espaços.
func splitEvents(csv string) []string {
	var out []string
	for _, p := range strings.Split(csv, ",") {
		if e := strings.TrimSpace(p); e != "" {
			out = append(out, e)
		}
	}
	return out
}

// webhookView é a representação devolvida por POST/GET do webhook. Nunca inclui
// o secret em claro, só se há um configurado.
func webhookView(sess *Session) map[string]any {
	ev := sess.getWebhookEvents()
	if ev == nil {
		ev = []string{}
	}
	return map[string]any{"webhook": sess.getWebhook(), "hasSecret": sess.getWebhookSecret() != "", "events": ev}
}

func (s *server) handleGetWebhook(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	writeJSON(w, http.StatusOK, webhookView(sess))
}

func (s *server) handleDeleteWebhook(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	sess.setWebhook("", "", nil)
	_ = sess.mgr.store.setWebhook(r.Context(), sess.id, "", "", "")
	w.WriteHeader(http.StatusNoContent)
}
