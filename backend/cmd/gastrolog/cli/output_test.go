package cli

import (
	"encoding/json"
	"testing"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestIsIDField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field string
		want  bool
	}{
		{"id", "id", true},
		{"snake case", "vault_id", true},
		{"protojson camelCase", "storageId", true},
		{"protojson policy", "rotationPolicyId", true},
		{"protojson nested", "retentionPolicyId", true},
		{"protojson chunk", "chunkId", true},
		{"protojson repeated", "nodeIds", true},
		{"snake repeated", "node_ids", true},
		{"not an id field", "name", false},
		{"not an id field", "enabled", false},
		{"not an id field", "storageClass", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isIDField(tt.field); got != tt.want {
				t.Errorf("isIDField(%q) = %v, want %v", tt.field, got, tt.want)
			}
		})
	}
}

func TestFormatIDBytes(t *testing.T) {
	t.Parallel()

	g := glid.New()
	want := g.String()

	if got := formatIDBytes(g.Bytes()); got != want {
		t.Errorf("raw GLID bytes: got %q, want %q", got, want)
	}
	if got := formatIDBytes([]byte(want)); got != want {
		t.Errorf("ASCII GLID string bytes: got %q, want %q", got, want)
	}
	if got := formatIDBytes(nil); got != "" {
		t.Errorf("nil: got %q, want empty", got)
	}
}

func TestConvertGLIDFieldsVaultConfigJSON(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	rotationID := glid.New()
	cloudID := glid.New()

	cfg := &v1.VaultConfig{
		Id:               vaultID.Bytes(),
		Name:             "test-vault",
		RotationPolicyId: rotationID.Bytes(),
		CloudServiceId:   cloudID.Bytes(),
	}

	raw, err := protojson.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}

	converted := convertGLIDFields(raw)

	var doc map[string]any
	if err := json.Unmarshal(converted, &doc); err != nil {
		t.Fatal(err)
	}

	for field, want := range map[string]string{
		"id":               vaultID.String(),
		"rotationPolicyId": rotationID.String(),
		"cloudServiceId":   cloudID.String(),
	} {
		got, ok := doc[field].(string)
		if !ok {
			t.Fatalf("%s: expected string, got %T", field, doc[field])
		}
		if got != want {
			t.Errorf("%s: got %q, want %q", field, got, want)
		}
	}
}

func TestConvertGLIDFieldsNodeIDsArray(t *testing.T) {
	t.Parallel()

	nodeA := glid.New()
	nodeB := glid.New()

	raw := []byte(`{"nodeIds":["` + mustBase64GLID(t, nodeA) + `","` + mustBase64GLID(t, nodeB) + `"]}`)
	converted := convertGLIDFields(raw)

	var doc struct {
		NodeIDs []string `json:"nodeIds"`
	}
	if err := json.Unmarshal(converted, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.NodeIDs) != 2 {
		t.Fatalf("nodeIds len = %d, want 2", len(doc.NodeIDs))
	}
	if doc.NodeIDs[0] != nodeA.String() || doc.NodeIDs[1] != nodeB.String() {
		t.Errorf("nodeIds = %v, want [%s %s]", doc.NodeIDs, nodeA, nodeB)
	}
}

func mustBase64GLID(t *testing.T, g glid.GLID) string {
	t.Helper()
	b, err := json.Marshal(g.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	return s
}
