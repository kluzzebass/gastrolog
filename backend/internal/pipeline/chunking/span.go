package chunking

import (
	"errors"
	"fmt"

	"gastrolog/internal/glid"
)

var (
	// ErrEmptySpan is returned when a span has zero records.
	ErrEmptySpan = errors.New("span count must be positive")
	// ErrSpanBounds is returned when start+count exceeds the segment's EventID order length.
	ErrSpanBounds = errors.New("span exceeds segment EventID order length")
)

// Span names a slice of one segment in EventID order (design-notes point 25).
// Start and Count are positions in EventID order, not on-disk frame offsets.
type Span struct {
	SegmentID glid.GLID
	Start     uint32
	Count     uint32
}

func (s Span) validate(orderLen uint32) error {
	if s.Count == 0 {
		return ErrEmptySpan
	}
	end, err := spanEnd(s.Start, s.Count)
	if err != nil {
		return err
	}
	if end > orderLen {
		return fmt.Errorf("%w: segment %s has %d EventID-ordered records, span [%d:%d)",
			ErrSpanBounds, s.SegmentID, orderLen, s.Start, end)
	}
	return nil
}

func spanEnd(start, count uint32) (uint32, error) {
	if count == 0 {
		return start, nil
	}
	end := start + count
	if end < start {
		return 0, fmt.Errorf("span overflow: start=%d count=%d", start, count)
	}
	return end, nil
}

// SpanRef binds a span to an on-disk segment file path.
type SpanRef struct {
	Path string
	Span Span
}
