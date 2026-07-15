package collection

import "gastrolog/internal/logging/comp"

var compCollection = comp.Root("pipeline").Sub("collection").Desc(
	"Per-home segment collection — pull assigned segments from peers into head/.")
