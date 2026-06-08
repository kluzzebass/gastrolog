package routing

// newAckJoin builds n per-target ack channels that join into a single parent
// ack. A collector goroutine reads exactly one result from each child and fires
// the parent once: nil only if every child committed, otherwise the first
// non-nil error. The caller must send exactly one value to each returned channel
// (the segmentation writers do on commit; route() nacks any it fails to deliver),
// so the collector always completes and never leaks.
func newAckJoin(n int, parent chan<- error) []chan<- error {
	chs := make([]chan error, n)
	out := make([]chan<- error, n)
	for i := range chs {
		chs[i] = make(chan error, 1)
		out[i] = chs[i]
	}
	go func() {
		var firstErr error
		for _, ch := range chs {
			if err := <-ch; err != nil && firstErr == nil {
				firstErr = err
			}
		}
		parent <- firstErr
	}()
	return out
}
