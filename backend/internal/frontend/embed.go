// Package frontend embeds the built frontend assets into the Go binary.
package frontend

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

// Handler returns an http.Handler that serves the embedded frontend assets,
// or nil if no real assets are embedded (dev mode — dist only contains .gitignore).
func Handler() *staticHandler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		return nil
	}

	// Check if there are any real files (not just .gitignore).
	hasContent := false
	fs.WalkDir(sub, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && path != ".gitignore" {
			hasContent = true
			return fs.SkipAll
		}
		return nil
	})

	if !hasContent {
		return nil
	}

	return newStaticHandler(sub)
}
