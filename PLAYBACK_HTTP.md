# Playback e transferência

Endpoints POST sob `/api/sessions/{sid}/calls/{id}/`:

- `announce` e `accept-announce`: request `{ "audioBase64": "<WAV em base64>", "hangupAfter": false, "maxSeconds": 120 }`. Somente `audioBase64` é obrigatório; o BFF deve fornecer o WAV. `maxSeconds` aceita inteiros de 1 a 300. Response 202 `{ "status": "playing", "call": { "callId": "<id>" }, "hangupAfter": false, "maxSeconds": 120 }`. Dono humano, browser ou playback existente causam 409.
- `hold-music`: request `{ "audioBase64": "<WAV em base64>" }`. Response 200 `{ "status": "ok", "held": true, "moh": true }`. Chamada precisa estar ativa/em espera; announce em andamento causa 409 e mantém seu playback e hangupAfter. Expiração sem mídia registra warning.
- `transfer`: request opcional `{ "to": "<clientId>" }`. Response 200 `{ "status": "ok", "transferred": true, "to": "<clientId ou vazio>" }`. Registro ausente no broker retorna 404 antes de colocar em espera ou desanexar o browser.

Playback aceita WAV PCM conforme validação do servidor (limite de 30 MiB). JSON desconhecido, base64/WAV inválido, corpo excessivo ou duração acima de maxSeconds retornam 400 `{ "error": "<motivo>" }`; conflitos retornam 409 no mesmo formato. Não há download remoto de áudio. Sessão/chamada inexistente retorna 404.
