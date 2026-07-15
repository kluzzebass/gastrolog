package segmentation

import "gastrolog/internal/logging/comp"

var compSegmentation = comp.Root("pipeline").Sub("segmentation").Desc(
	"Per-vault segment writers — durable working/ appends, group-commit fsync, complete-policy rotation, and crash recovery of orphaned working segments.")
