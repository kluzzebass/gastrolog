package chunking

import "gastrolog/internal/logging/comp"

var compChunking = comp.Root("pipeline").Sub("chunking").Desc(
	"Per-home chunking — leader manifest planning, GLCB build at seal, holder-gated segment release.")
