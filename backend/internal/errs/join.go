package errs

import (
	"fmt"
	"strings"
)

type joined struct {
	msg    string
	unwrap []error
}

func (e *joined) Error() string   { return e.msg }
func (e *joined) Unwrap() []error { return e.unwrap }

// SummaryJoin combines non-nil errors into one message suitable for logs and
// RPC surfaces. Unlike errors.Join, identical messages are deduplicated with
// a count and distinct messages are separated by "; " (not newlines).
func SummaryJoin(errs ...error) error {
	flat := compact(errs)
	if len(flat) == 0 {
		return nil
	}
	if len(flat) == 1 {
		return flat[0]
	}

	type bucket struct {
		err   error
		count int
	}
	var buckets []bucket
	idx := make(map[string]int, len(flat))
	for _, err := range flat {
		key := err.Error()
		if i, ok := idx[key]; ok {
			buckets[i].count++
			continue
		}
		idx[key] = len(buckets)
		buckets = append(buckets, bucket{err: err, count: 1})
	}

	parts := make([]string, 0, len(buckets))
	unwrap := make([]error, 0, len(buckets))
	for _, b := range buckets {
		unwrap = append(unwrap, b.err)
		if b.count > 1 {
			parts = append(parts, fmt.Sprintf("%s (×%d)", b.err.Error(), b.count))
			continue
		}
		parts = append(parts, b.err.Error())
	}
	return &joined{msg: strings.Join(parts, "; "), unwrap: unwrap}
}

func compact(errs []error) []error {
	out := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}
