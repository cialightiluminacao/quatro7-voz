# Motor de voz: estabilizar o pareamento (22/09/2026)

## Problema
Ao parear a voz (QR), o celular fica carregando e mostra "Não foi possível conectar o
dispositivo. Tente novamente mais tarde." Enquanto isso o Quatro7 mostra a caixa como
conectada.

Medido em 22/09 na voz-cx16 (8611): o motor recebeu a leitura do QR nas duas tentativas
(15:07Z e 15:12Z) e marcou a sessão como `open`, mas o celular nunca concluiu o vínculo
(`whatsmeow_app_state_sync_keys` = 0; as três sessões sadias têm >= 1).

## Causas
1. whatsmeow fixado em b572e5b (30/06), versão web 2.3000.1042386815. O upstream tem
   68 commits depois, incluindo três no caminho do pareamento (37a86e6 client props,
   9ec8f76 companion_reg_refresh, 60027b0 return faltando no qrchan). Causa provável,
   não provada.
2. O motor não define DeviceProps: apresenta-se como `Os: "whatsmeow"`.
3. Estado mentiroso: `open` logo no pair-success; Disconnected, KeepAliveTimeout e
   StreamReplaced não mudavam o estado; logger do whatsmeow era Noop.

## Decisão (Eduardo, 22/09): C = A agora, B depois

### Fase A: fork atual (frota5)
- whatsmeow para o HEAD upstream; ajustar quebras de API.
- DeviceProps: Os "Quatro7 Voz", PlatformType DESKTOP.
- Estado honesto (commit cc9ceaf): `linking` até o celular entregar a chave de app
  state; `connecting` na queda; `replaced` no stream replaced; logs de pareamento;
  whatsmeow em WARN.
- O BFF não muda: estado desconhecido com `paired` já vira "conectando", e o watchdog
  avisa os admins depois de ~10 min.
- Deploy: imagem `q7calls:v0.0.6-frota5-<data>` em cima da frota4 (troca só o binário),
  container anterior renomeado e parado como rollback, troca feita sem ligação ativa.
- Validação: as 4 sessões voltam `open` com `linked=true`; cx16 re-pareada pelo celular
  conclui o vínculo (`linked=true`, chave > 0).

### Fase B: rebase sobre AstraCalls v0.0.9 (depois)
Ganha gateway SIP/PBX (v0.0.8), correção de vazamento entre empresas (v0.0.8),
"aparelho inativo com conexão viva" (v0.0.7), recusa e encerramento em outro aparelho
(v0.0.9). Conflita com os nossos fixes de chamada-fantasma. Testar na caixa 9 antes da
frota.

### Robustez permanente
- Alerta quando a versão web do motor ficar mais de 30 dias atrás do upstream.
- Rollback de 30 s sempre disponível (container anterior parado).
- Deploy do motor fora de ligação ativa.
