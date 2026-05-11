package orchestrator

import "gastrolog/internal/glid"

// LastSeen is the test-exposed alias for the unexported lastSeen tracker
// used by runChunkProgressEmitter. Tests construct an empty map of this
// type and pass it to EmitActiveChunkProgress to drive single-tick
// behavior deterministically (without standing up a real ticker).
type LastSeen = lastSeen

// NewLastSeenMap returns a fresh tracker map for the progress emitter,
// suitable for passing to EmitActiveChunkProgress.
func NewLastSeenMap() map[glid.GLID]LastSeen {
	return make(map[glid.GLID]LastSeen)
}

// EmitActiveChunkProgress drives one tick of the progress emitter
// against the provided tracker. Exported here so the orchestrator_test
// package can pin the emit-when-advanced / no-emit-when-idle / reset-
// on-rotation contract without waiting for the real time.NewTicker
// inside runChunkProgressEmitter.
func (o *Orchestrator) EmitActiveChunkProgress(last map[glid.GLID]LastSeen) {
	o.emitActiveChunkProgress(last)
}
