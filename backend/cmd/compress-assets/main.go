// Command compress-assets walks a directory, creates brotli-compressed (.br) siblings
// for every file, then removes the originals. Used at build time to prepare frontend
// assets for embedding — only .br files are kept in the binary.
package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/andybalholm/brotli"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: compress-assets <dir>\n")
		os.Exit(1)
	}
	dir := os.Args[1]

	// Collect files first (avoid walking while mutating).
	var files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Base(path) != ".gitignore" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "walk: %v\n", err)
		os.Exit(1)
	}

	for _, path := range files {
		if err := compressFile(path); err != nil {
			fmt.Fprintf(os.Stderr, "compress %s: %v\n", path, err)
			os.Exit(1)
		}
	}

	fmt.Fprintf(os.Stderr, "compressed %d files\n", len(files))
}

func compressFile(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	dst, err := os.Create(path + ".br")
	if err != nil {
		return err
	}
	defer dst.Close()

	w := brotli.NewWriterLevel(dst, brotli.BestCompression)
	if _, err := w.Write(src); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	if err := dst.Close(); err != nil {
		return err
	}

	return os.Remove(path)
}
