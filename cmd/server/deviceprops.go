package main

import (
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

// Identidade do aparelho vinculado, enviada ao WhatsApp só no pareamento (sessões
// já pareadas não mudam). O padrão do whatsmeow é Os "whatsmeow" com plataforma
// genérica; apresentamos um desktop com nome próprio, como faz o WAHA.
//
// Histórico: o padrão pede até 10 GB sem limite de dias. Num número grande (o 8611
// da Cia Light, iPhone Business) o celular ficava minutos empacotando o histórico
// inicial para o aparelho novo e desistia com "Não foi possível conectar o
// dispositivo". A voz não usa histórico: pedimos o mínimo.
func init() {
	store.SetOSInfo("Quatro7 Voz", [3]uint32{1, 0, 0})
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
	store.DeviceProps.RequireFullSync = proto.Bool(false)

	h := store.DeviceProps.HistorySyncConfig
	h.FullSyncDaysLimit = proto.Uint32(1)
	h.FullSyncSizeMbLimit = proto.Uint32(1)
	h.RecentSyncDaysLimit = proto.Uint32(1)
	h.StorageQuotaMb = proto.Uint32(64)
	h.ThumbnailSyncDaysLimit = proto.Uint32(0)
	h.InitialSyncMaxMessagesPerChat = proto.Uint32(1)
}
