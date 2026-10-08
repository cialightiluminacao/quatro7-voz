# Modificações deste fork

Este repositório é um fork do [AstraCalls](https://github.com/AstraOnlineWeb/AstraCalls), distribuído sob a mesma licença (AGPL-3.0, ver `LICENSE`). É o motor de ligações de voz e vídeo de WhatsApp usado pelo quatro7.

Principais mudanças em relação ao upstream (detalhes no histórico de commits):
- codec MLow serializado e compilado sem `OPUS_HARDENING`, para que um pacote malformado não derrube o processo;
- fila de reordenação do áudio de entrada, timeout atômico e descarte antes da decodificação, contra o áudio que sumia no meio da ligação;
- contrato de vídeo com orientação da câmera, pedido de quadro-chave e estado da câmera de cada lado;
- anúncio sem navegador (mensagem de fora do horário com desligamento), música de espera e transferência entre atendentes (ver `PLAYBACK_HTTP.md`);
- recusa e desligamento que voltam a chegar ao WhatsApp, e o mapa de LID.
