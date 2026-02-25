package querylang

import (
	"sort"
	"strings"
)

// FieldsAtCursor returns the available fields and context completions at the
// given cursor position within a pipeline expression.
//
// It strips directives, lexes to find pipe boundaries, parses each operator
// (tolerant of partial input), and walks operators applying field transformations
// up to the segment containing the cursor.
//
// baseFields are the fields the frontend has seen from search results.
func FieldsAtCursor(expr string, cursor int, baseFields []string) (fields, completions []string) {
	if len(expr) == 0 {
		return baseFields, nil
	}

	// Strip comments first, then directives.
	commentStripped, commentRanges := stripCommentsWithRanges(expr)
	stripped, removedRanges := stripDirectives(commentStripped)

	// Map cursor from original expression through comment stripping, then directive stripping.
	cursorAfterComments := min(max(mapOffsetToStripped(cursor, commentRanges), 0), len(commentStripped))
	strippedCursor := min(max(mapOffsetToStripped(cursorAfterComments, removedRanges), 0), len(stripped))

	// Lex to find pipe positions.
	pipePositions := findPipePositions(stripped)

	// Determine which segment the cursor is in.
	// Segments: [0, pipe0), [pipe0+1, pipe1), [pipe1+1, pipe2), ...
	segmentIdx := 0
	for i, pp := range pipePositions {
		if strippedCursor > pp {
			segmentIdx = i + 1
		}
	}

	// The cursor is in the filter segment (before first pipe) — return base fields.
	if segmentIdx == 0 {
		return sortedUnique(baseFields), nil
	}

	// Try to parse the pipeline. If full parse fails (e.g. trailing "| "),
	// try parsing up to the last pipe before the cursor's segment.
	pipeline := parseTolerant(stripped, pipePositions, segmentIdx)

	// Start with base fields.
	currentFields := makeFieldSet(baseFields)

	if pipeline == nil || len(pipeline.Pipes) == 0 {
		// No successfully parsed operators — return base fields.
		return sortedUnique(baseFields), contextCompletionsForSegment(stripped, pipePositions, segmentIdx)
	}

	// Walk operators up to (but not including) the segment containing the cursor.
	// segmentIdx-1 is the pipe operator index (0-indexed).
	opIdx := segmentIdx - 1 // which pipe operator the cursor is in
	for i := 0; i < opIdx && i < len(pipeline.Pipes); i++ {
		currentFields = applyOperator(pipeline.Pipes[i], currentFields)
	}

	// Generate completions based on the current operator keyword.
	var comp []string
	if opIdx < len(pipeline.Pipes) {
		comp = completionsForOperator(pipeline.Pipes[opIdx])
	} else {
		comp = contextCompletionsForSegment(stripped, pipePositions, segmentIdx)
	}

	return sortedUnique(fieldSetToSlice(currentFields)), comp
}

// mapOffsetToStripped maps a byte offset in the original expression to the
// corresponding offset in the stripped expression.
func mapOffsetToStripped(originalPos int, removed []directiveRange) int {
	strippedPos := originalPos
	for _, r := range removed {
		rangeLen := r.end - r.start
		if r.start < originalPos {
			if r.end <= originalPos {
				strippedPos -= rangeLen
			} else {
				// Cursor is inside the removed range — map to start.
				strippedPos -= (originalPos - r.start)
			}
		}
	}
	return strippedPos
}

// parseTolerant tries to parse the pipeline. If the full expression fails
// (common during typing — trailing "| " has no operator), it truncates to
// only include complete segments before the cursor and tries again.
func parseTolerant(stripped string, pipePositions []int, segmentIdx int) *Pipeline {
	// First, try parsing the full expression.
	pipeline, err := ParsePipeline(stripped)
	if err == nil {
		return pipeline
	}

	// Full parse failed — try truncating at each pipe boundary, from the
	// one before the cursor's segment down to the first pipe, until a parse succeeds.
	for truncAt := segmentIdx - 1; truncAt >= 0; truncAt-- {
		if truncAt >= len(pipePositions) {
			continue
		}
		truncated := strings.TrimSpace(stripped[:pipePositions[truncAt]])
		if truncated == "" {
			continue
		}
		pipeline, err := ParsePipeline(truncated)
		if err == nil {
			return pipeline
		}
	}

	return nil
}

// findPipePositions lexes the expression and returns byte offsets of pipe tokens.
func findPipePositions(input string) []int {
	lex := NewLexer(input)
	var positions []int
	for {
		tok, err := lex.Next()
		if err != nil || tok.Kind == TokEOF {
			break
		}
		if tok.Kind == TokPipe {
			positions = append(positions, tok.Pos)
		}
	}
	return positions
}

type fieldSet map[string]bool

func makeFieldSet(fields []string) fieldSet {
	fs := make(fieldSet, len(fields))
	for _, f := range fields {
		fs[f] = true
	}
	return fs
}

func fieldSetToSlice(fs fieldSet) []string {
	s := make([]string, 0, len(fs))
	for f := range fs {
		s = append(s, f)
	}
	return s
}

// applyOperator transforms the field set based on a pipe operator.
func applyOperator(op PipeOp, fields fieldSet) fieldSet {
	switch o := op.(type) {
	case *EvalOp:
		// Eval adds new fields (or overwrites existing ones).
		result := copyFieldSet(fields)
		for _, a := range o.Assignments {
			result[a.Field] = true
		}
		return result

	case *RenameOp:
		result := copyFieldSet(fields)
		for _, r := range o.Renames {
			if result[r.Old] {
				delete(result, r.Old)
				result[r.New] = true
			}
		}
		return result

	case *FieldsOp:
		if o.Drop {
			// Drop mode: remove listed fields.
			result := copyFieldSet(fields)
			for _, name := range o.Names {
				delete(result, name)
			}
			return result
		}
		// Keep mode: only keep listed fields.
		result := make(fieldSet, len(o.Names))
		for _, name := range o.Names {
			result[name] = true
		}
		return result

	case *StatsOp:
		// Stats replaces the entire schema.
		result := make(fieldSet)
		for _, g := range o.Groups {
			if g.Field != nil {
				result[g.Field.Name] = true
			}
			if g.Bin != nil {
				// bin() produces a "_time" column.
				result["_time"] = true
			}
		}
		for _, a := range o.Aggs {
			result[a.DefaultAlias()] = true
		}
		return result

	case *TimechartOp:
		// Timechart replaces schema: _time + count (or series values).
		result := make(fieldSet)
		result["_time"] = true
		result["count"] = true
		if o.By != "" {
			// When grouped, each value becomes a column — we can't know at parse time.
			// Just add the group field.
			result[o.By] = true
		}
		return result

	case *WhereOp, *SortOp, *HeadOp, *TailOp, *SliceOp, *RawOp:
		// Pass-through operators don't change the field set.
		return fields

	default:
		return fields
	}
}

func copyFieldSet(fs fieldSet) fieldSet {
	result := make(fieldSet, len(fs))
	for f := range fs {
		result[f] = true
	}
	return result
}

// completionsForOperator returns context-specific keywords for the operator
// the cursor is currently inside.
func completionsForOperator(op PipeOp) []string {
	switch op.(type) {
	case *StatsOp:
		return []string{"by", "as"}
	case *RenameOp:
		return []string{"as"}
	case *TimechartOp:
		return []string{"by"}
	default:
		return nil
	}
}

// contextCompletionsForSegment extracts the keyword from the segment text
// and returns appropriate completions.
func contextCompletionsForSegment(stripped string, pipePositions []int, segmentIdx int) []string {
	if segmentIdx <= 0 || segmentIdx > len(pipePositions) {
		return nil
	}

	segStart := pipePositions[segmentIdx-1] + 1
	var segEnd int
	if segmentIdx < len(pipePositions) {
		segEnd = pipePositions[segmentIdx]
	} else {
		segEnd = len(stripped)
	}

	segment := strings.TrimSpace(stripped[segStart:segEnd])
	keyword := strings.Fields(segment)
	if len(keyword) == 0 {
		return nil
	}

	switch strings.ToLower(keyword[0]) {
	case "stats":
		return []string{"by", "as"}
	case "rename":
		return []string{"as"}
	case "timechart":
		return []string{"by"}
	default:
		return nil
	}
}

func sortedUnique(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(s))
	result := make([]string, 0, len(s))
	for _, v := range s {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	sort.Strings(result)
	return result
}
