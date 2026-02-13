package tail

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
)

// discoverFiles returns deduplicated absolute paths of regular files matching
// any of the given glob patterns.
func discoverFiles(patterns []string) ([]string, error) {
	seen := make(map[string]bool)
	var result []string

	for _, pattern := range patterns {
		// Make pattern absolute for consistent paths.
		if !filepath.IsAbs(pattern) {
			wd, err := os.Getwd()
			if err != nil {
				return nil, err
			}
			pattern = filepath.Join(wd, pattern)
		}

		matches, err := doublestar.FilepathGlob(pattern)
		if err != nil {
			return nil, err
		}

		for _, m := range matches {
			abs, err := filepath.Abs(m)
			if err != nil {
				continue
			}
			info, err := os.Stat(abs)
			if err != nil || !info.Mode().IsRegular() {
				continue
			}
			if !seen[abs] {
				seen[abs] = true
				result = append(result, abs)
			}
		}
	}

	return result, nil
}

// watchDirsForPatterns extracts the static directory prefixes from glob patterns
// for use with fsnotify directory watching. It returns the longest prefix before
// the first glob metacharacter in each pattern.
func watchDirsForPatterns(patterns []string) []string {
	seen := make(map[string]bool)
	var dirs []string

	for _, pattern := range patterns {
		if !filepath.IsAbs(pattern) {
			if wd, err := os.Getwd(); err == nil {
				pattern = filepath.Join(wd, pattern)
			}
		}

		dir := staticPrefix(pattern)
		if !seen[dir] {
			seen[dir] = true
			dirs = append(dirs, dir)
		}
	}

	return dirs
}

// staticPrefix returns the longest directory path before the first glob character.
func staticPrefix(pattern string) string {
	// Find first glob metacharacter.
	for i, c := range pattern {
		if c == '*' || c == '?' || c == '[' || c == '{' {
			// Return the directory containing this position.
			return filepath.Dir(pattern[:i])
		}
	}
	// No glob characters — pattern is a literal file path; watch its directory.
	return filepath.Dir(pattern)
}

// matchesAnyPattern reports whether path matches any of the given glob patterns.
func matchesAnyPattern(path string, patterns []string) bool {
	for _, pattern := range patterns {
		if !filepath.IsAbs(pattern) {
			if wd, err := os.Getwd(); err == nil {
				pattern = filepath.Join(wd, pattern)
			}
		}
		if ok, _ := doublestar.PathMatch(pattern, path); ok {
			return true
		}
		// Also try Match which handles ** differently.
		if strings.Contains(pattern, "**") {
			if ok, _ := doublestar.Match(pattern, path); ok {
				return true
			}
		}
	}
	return false
}
