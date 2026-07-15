package cluster

import "gastrolog/internal/logging/comp"

var compPeerConns = comp.Root("cluster").Sub("peer-conns").Desc(
	"Outbound peer connection manager — service-lane pools and raft-lane singletons per peer.")
