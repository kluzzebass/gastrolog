package alert

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Raise takes a plain string, so raising a type ID that has no catalog entry
// compiles and runs. It does not fail silently — the collector stamps it with
// unregisteredAlarmType — but what the operator then sees is a software fault
// reading "a component raised an alarm type that is not in the alarm catalog",
// with no priority and no guidance, in place of the real condition. Nothing
// caught that until this test: seal-stranded and vault-announce-failing both
// shipped uncataloged.
//
// This walks the source rather than the runtime because most raise sites need a
// live cluster to reach.
func TestEveryRaisedAlarmTypeIsCataloged(t *testing.T) {
	t.Parallel()

	raises, unresolved := scanRaiseSites(t)

	// Guard the guard: if the resolver stops recognizing call sites this test
	// passes vacuously. The tree had 41 raise sites when this was written and
	// all but the two dynamic ones resolve.
	const minResolved = 30
	if len(raises) < minResolved {
		t.Fatalf("resolved only %d raise sites (want >= %d) — the scanner is no longer finding them, so this test proves nothing",
			len(raises), minResolved)
	}

	for id, where := range raises {
		if _, ok := TypeByID(id); !ok {
			t.Errorf("alarm type %q is raised at %s but has no catalog entry — it will annunciate as a software fault instead of the real condition",
				id, where)
		}
	}

	// Dynamic IDs cannot be checked statically. Name them so the gap is
	// visible rather than implied. The rate alerter is the only one, and it
	// guards itself by panicking at construction on an uncataloged kind.
	for _, u := range unresolved {
		t.Logf("raise site with a non-literal type ID, not covered by this test: %s", u)
	}
}

// A GLID is 26 characters of base32 that an operator cannot match to anything
// they configured. Alarm detail names entities by name (alert.Label) and leaves
// the ID to the instance key, which is a dedup key and never shown as prose.
// Truncating the ID for display is worse than either: not recognizable, and not
// pasteable into a CLI. Scoped to the detail argument — a truncated ID in a log
// line or a UI column is a different call.
func TestNoAlarmDetailFormatsATruncatedID(t *testing.T) {
	t.Parallel()
	for _, hit := range truncatedIDsInDetails(t) {
		t.Errorf("%s truncates an ID inside the alarm detail — name the entity via alert.Label instead", hit)
	}
}

// forEachRaiseSite parses the non-test backend tree and calls fn for every
// three-argument .Raise(typeID, instanceKey, detail) call, with the package's
// string constants for resolving a non-literal type ID.
func forEachRaiseSite(t *testing.T, fn func(where string, args []ast.Expr, consts map[string]string)) {
	t.Helper()

	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve backend root: %v", err)
	}

	fset := token.NewFileSet()
	var dirs []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".") {
				return fs.SkipDir
			}
			dirs = append(dirs, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	for _, dir := range dirs {
		pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool { //nolint:staticcheck // build tags are irrelevant here; every raise site is in an untagged file
			return !strings.HasSuffix(fi.Name(), "_test.go")
		}, 0)
		if err != nil {
			// A directory that does not parse as Go is not this test's problem.
			continue
		}
		for _, pkg := range pkgs {
			consts := stringConsts(pkg)
			for _, file := range pkg.Files {
				ast.Inspect(file, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "Raise" || len(call.Args) != 3 {
						return true
					}
					pos := fset.Position(call.Pos())
					where := filepath.Base(filepath.Dir(pos.Filename)) + "/" + filepath.Base(pos.Filename) + ":" + strconv.Itoa(pos.Line)
					fn(where, call.Args, consts)
					return true
				})
			}
		}
	}
}

// scanRaiseSites returns type ID -> first location, plus the locations whose
// type ID is not a string literal or a package const.
func scanRaiseSites(t *testing.T) (map[string]string, []string) {
	t.Helper()
	raises := map[string]string{}
	var unresolved []string
	forEachRaiseSite(t, func(where string, args []ast.Expr, consts map[string]string) {
		id, ok := resolveTypeID(args[0], consts)
		if !ok {
			unresolved = append(unresolved, where)
			return
		}
		if _, seen := raises[id]; !seen {
			raises[id] = where
		}
	})
	return raises, unresolved
}

// truncatedIDsInDetails returns raise sites whose detail argument slices the
// result of a .String() call — the "first 8 characters of the GLID" pattern.
func truncatedIDsInDetails(t *testing.T) []string {
	t.Helper()
	var hits []string
	forEachRaiseSite(t, func(where string, args []ast.Expr, _ map[string]string) {
		ast.Inspect(args[2], func(n ast.Node) bool {
			slice, ok := n.(*ast.SliceExpr)
			if !ok {
				return true
			}
			call, ok := slice.X.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "String" {
				hits = append(hits, where)
			}
			return true
		})
	})
	return hits
}

// stringConsts collects package-level string constants with literal values.
func stringConsts(pkg *ast.Package) map[string]string {
	out := map[string]string{}
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					if v, ok := literalString(vs.Values[i]); ok {
						out[name.Name] = v
					}
				}
			}
		}
	}
	return out
}

func resolveTypeID(arg ast.Expr, consts map[string]string) (string, bool) {
	if v, ok := literalString(arg); ok {
		return v, true
	}
	if ident, ok := arg.(*ast.Ident); ok {
		if v, ok := consts[ident.Name]; ok {
			return v, true
		}
	}
	return "", false
}

func literalString(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return v, true
}
