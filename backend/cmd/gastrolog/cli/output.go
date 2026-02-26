package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
)

// printer handles table or JSON output.
type printer struct {
	format string
	w      io.Writer
}

func newPrinter(format string) *printer {
	return &printer{format: format, w: os.Stdout}
}

// json marshals v as indented JSON.
func (p *printer) json(v any) error {
	enc := json.NewEncoder(p.w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// table writes rows using tabwriter. header is the first row.
// rows is a slice of slices; each inner slice is a row of strings.
func (p *printer) table(header []string, rows [][]string) {
	tw := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)
	for i, h := range header {
		if i > 0 {
			_, _ = fmt.Fprint(tw, "\t")
		}
		_, _ = fmt.Fprint(tw, h)
	}
	_, _ = fmt.Fprintln(tw)
	for _, row := range rows {
		for i, col := range row {
			if i > 0 {
				_, _ = fmt.Fprint(tw, "\t")
			}
			_, _ = fmt.Fprint(tw, col)
		}
		_, _ = fmt.Fprintln(tw)
	}
	_ = tw.Flush()
}

// kv prints a key-value detail view.
func (p *printer) kv(pairs [][2]string) {
	tw := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)
	for _, pair := range pairs {
		_, _ = fmt.Fprintf(tw, "%s:\t%s\n", pair[0], pair[1])
	}
	_ = tw.Flush()
}
