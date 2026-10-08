package main

import (
	"wacalls/internal/voip/wanode"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func callIDFromNode(node *waBinary.Node) string {
	if node != nil {
		for _, child := range node.GetChildren() {
			if id := wanode.AttrString(child.Attrs, "call-id"); id != "" {
				return id
			}
		}
	}
	return ""
}

// callActionNode preserva os atributos externos (from/id) e seleciona a ação
// mesmo quando o whatsmeow entrega vários filhos como UnknownCallEvent.
func callActionNode(node *waBinary.Node, tags ...string) *waBinary.Node {
	if node == nil {
		return nil
	}
	for _, child := range node.GetChildren() {
		for _, tag := range tags {
			if child.Tag == tag {
				selected := *node
				selected.Content = []waBinary.Node{child}
				return &selected
			}
		}
	}
	return nil
}
