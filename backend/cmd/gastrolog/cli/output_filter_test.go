package cli

// A filter argument or flag narrows the table and the JSON output alike.
// Automation reads `-o json`, so a filter honored only by the table makes a
// script act on entities the operator excluded — or, when it greps for the
// absence of an entry, skip the one it was asked about.

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func runConfigCaptured(t *testing.T, addr string, args ...string) string {
	t.Helper()
	cmd := NewConfigCommand()
	AddClientFlags(cmd)
	cmd.SetArgs(append(args, "--addr", addr))
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	var runErr error
	out := captureStdout(t, func() { runErr = cmd.Execute() })
	if runErr != nil {
		t.Fatalf("config %s: %v", strings.Join(args, " "), runErr)
	}
	return string(out)
}

// tableColumn returns column col of every data row of a table rendering.
func tableColumn(out string, col int) []string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var vals []string
	for _, line := range lines[1:] {
		if fields := strings.Fields(line); len(fields) > col {
			vals = append(vals, fields[col])
		}
	}
	return vals
}

func TestNodeListStorageFilterAppliesToEveryOutputFormat(t *testing.T) {
	ts, cfgStore := newCloudServiceTestServer(t)
	ctx := context.Background()

	alpha, bravo, charlie := glid.New(), glid.New(), glid.New()
	for _, n := range []system.NodeConfig{{ID: alpha, Name: "alpha"}, {ID: bravo, Name: "bravo"}, {ID: charlie, Name: "charlie"}} {
		if err := cfgStore.PutNode(ctx, n); err != nil {
			t.Fatalf("PutNode %s: %v", n.Name, err)
		}
	}
	for id, storage := range map[glid.GLID]string{alpha: "alpha-store", bravo: "bravo-store"} {
		if err := cfgStore.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
			NodeID:       id.String(),
			FileStorages: []system.FileStorage{{ID: glid.New(), Name: storage, Path: "/data/" + storage, StorageClass: 1}},
		}); err != nil {
			t.Fatalf("SetNodeStorageConfig %s: %v", storage, err)
		}
	}

	both := []string{"alpha-store", "bravo-store"}
	cases := []struct {
		name string
		args []string
		want []string
	}{
		{"all nodes", nil, both},
		{"by name", []string{"alpha"}, []string{"alpha-store"}},
		{"by name case-insensitive", []string{"BRAVO"}, []string{"bravo-store"}},
		{"by ID", []string{bravo.String()}, []string{"bravo-store"}},
		{"node without storage", []string{"charlie"}, nil},
	}
	for _, tc := range cases {
		t.Run("table/"+tc.name, func(t *testing.T) {
			out := runConfigCaptured(t, ts.URL, append([]string{"node", "list-storage"}, tc.args...)...)
			got := tableColumn(out, 2)
			slices.Sort(got)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("table storages = %v, want %v\n%s", got, tc.want, out)
			}
		})
		t.Run("json/"+tc.name, func(t *testing.T) {
			out := runConfigCaptured(t, ts.URL, append([]string{"node", "list-storage", "-o", "json"}, tc.args...)...)
			if !strings.HasPrefix(strings.TrimSpace(out), "[") {
				t.Fatalf("JSON output is not an array:\n%s", out)
			}
			var configs []struct {
				FileStorages []struct {
					Name string `json:"name"`
				} `json:"file_storages"`
			}
			if err := json.Unmarshal([]byte(out), &configs); err != nil {
				t.Fatalf("decode JSON: %v\n%s", err, out)
			}
			var got []string
			for _, c := range configs {
				for _, fs := range c.FileStorages {
					got = append(got, fs.Name)
				}
			}
			slices.Sort(got)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("JSON storages = %v, want %v\n%s", got, tc.want, out)
			}
		})
	}
}

func TestLogLevelComponentsPrefixAppliesToEveryOutputFormat(t *testing.T) {
	ts, _ := newCloudServiceTestServer(t)

	var all []struct {
		Path string `json:"path"`
	}
	allOut := runConfigCaptured(t, ts.URL, "log-level", "components", "-o", "json")
	if err := json.Unmarshal([]byte(allOut), &all); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, allOut)
	}
	if len(all) < 2 {
		t.Fatalf("need at least two registered components to observe filtering, got %d", len(all))
	}
	prefix := all[0].Path
	var want []string
	for _, c := range all {
		if strings.HasPrefix(c.Path, prefix) {
			want = append(want, c.Path)
		}
	}
	if len(want) == len(all) {
		t.Fatalf("prefix %q matches every component; it cannot observe filtering", prefix)
	}

	allPaths := make([]string, 0, len(all))
	for _, c := range all {
		allPaths = append(allPaths, c.Path)
	}
	if got := tableColumn(runConfigCaptured(t, ts.URL, "log-level", "components"), 0); !slices.Equal(got, allPaths) {
		t.Fatalf("unfiltered table paths = %v, want %v", got, allPaths)
	}

	tableOut := runConfigCaptured(t, ts.URL, "log-level", "components", "--prefix", prefix)
	if got := tableColumn(tableOut, 0); !slices.Equal(got, want) {
		t.Fatalf("table paths = %v, want %v", got, want)
	}

	jsonOut := runConfigCaptured(t, ts.URL, "log-level", "components", "--prefix", prefix, "-o", "json")
	var filtered []struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &filtered); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, jsonOut)
	}
	var got []string
	for _, c := range filtered {
		got = append(got, c.Path)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("JSON paths = %v, want %v", got, want)
	}
}
