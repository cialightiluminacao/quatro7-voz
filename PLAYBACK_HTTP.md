# Playback e transferência

A identidade de uma chamada é o par `(sessionId, callId)`. O WhatsApp pode usar
o mesmo `callId` nos dois lados de uma chamada entre sessões hospedadas no mesmo
processo. Todas as operações HTTP abaixo resolvem exclusivamente a sessão de
`/api/sessions/{sid}/calls/{id}/...`; o broker exige a sessão em consultas,
encerramento e operações de dono/claim. Eventos e respostas mantêm os campos e
formatos existentes (`id`, `callId`, `sessionId`).

Não há rotas legadas de chamada `/api/calls/{id}/...` registradas: elas retornam
404. Um chamador com somente o `callId` precisa obter o `sessionId` do evento ou
registro original; nunca deve procurar a primeira chamada com esse ID em outras
sessões. Hangup de uma chamada ausente na sessão indicada continua idempotente
(204), sem afetar uma chamada de mesmo ID em outra sessão.

Os índices locais por `callId` são intencionais: `Session.reg` é exclusivo de uma
sessão e contém seus timers, pontes WebRTC/WebSocket, codecs e alvo de
transferência. Os callbacks de `Bridge` capturam esse `activeCall`/`CallManager`.
`CallManager` pertence a uma chamada e mantém playback/announce/hold-music e
vídeo dentro dessa instância; a consulta privada usa a mesma instância, sem mapa
global de consult. O histórico do broker é uma lista de registros que preservam
ambos os IDs, filtrada por sessão, sem índice ou deduplicação por `callId`.

As gravações seguem `<callId>.mp3` (o BFF depende desse nome quando o webhook vem
sem url). Se o arquivo já existe — a outra ponta de uma chamada entre duas sessões
do mesmo processo —, a segunda grava `<callId>-<sessão>.mp3` sem sobrescrever; a url
do webhook traz o nome real.

Endpoints POST sob `/api/sessions/{sid}/calls/{id}/`:

- `announce` e `accept-announce`: request `{ "audioBase64": "<WAV em base64>", "hangupAfter": false, "maxSeconds": 120 }`. Somente `audioBase64` é obrigatório; o BFF deve fornecer o WAV. `maxSeconds` aceita inteiros de 1 a 300. Response 202 `{ "status": "playing", "call": { "callId": "<id>" }, "hangupAfter": false, "maxSeconds": 120 }`. Dono humano, browser ou playback existente causam 409.
- `hold-music`: request `{ "audioBase64": "<WAV em base64>" }`. Response 200 `{ "status": "ok", "held": true, "moh": true }`. Chamada precisa estar ativa/em espera; announce em andamento causa 409 e mantém seu playback e hangupAfter. Expiração sem mídia registra warning.
- `transfer`: request opcional `{ "to": "<clientId>" }`. Response 200 `{ "status": "ok", "transferred": true, "to": "<clientId ou vazio>" }`. Registro ausente no broker retorna 404 antes de colocar em espera ou desanexar o browser.

Playback aceita WAV PCM conforme validação do servidor (limite de 30 MiB). JSON desconhecido, base64/WAV inválido, corpo excessivo ou duração acima de maxSeconds retornam 400 `{ "error": "<motivo>" }`; conflitos retornam 409 no mesmo formato. Não há download remoto de áudio. Sessão/chamada inexistente retorna 404.
