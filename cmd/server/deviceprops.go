package main

import (
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

// Identidade do aparelho vinculado, enviada ao WhatsApp só no pareamento (sessões
// já pareadas não mudam). O padrão do whatsmeow é Os "whatsmeow" com plataforma
// genérica; apresentamos um desktop com nome próprio, como faz o WAHA.
func init() {
	store.SetOSInfo("Quatro7 Voz", [3]uint32{1, 0, 0})
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
	store.DeviceProps.RequireFullSync = proto.Bool(false)
}
