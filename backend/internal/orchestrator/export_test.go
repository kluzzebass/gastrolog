package orchestrator

// TryLockForTest reports whether the orchestrator lock is free right now,
// releasing it again when it was: a test's probe that a long-running
// operation is not holding it.
func (o *Orchestrator) TryLockForTest() bool {
	if !o.mu.TryLock() {
		return false
	}
	o.mu.Unlock()
	return true
}
