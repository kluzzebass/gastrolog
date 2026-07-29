package cli

// The round-trip test enumerates sections by name. That guard is only as good
// as the list: a config type added to system.Config and to neither list is
// invisible to it, which is exactly how every enrichment lookup table came to
// be absent from the document — unexported, unimported, and not declared in
// `excluded` either (gastrolog-4j7srt).
//
// This test derives the expectation from system.Config instead, so the next
// type added fails here rather than on a live cluster during a restore.

import (
	"reflect"
	"strings"
	"testing"

	"gastrolog/internal/system"
)

// configFieldToSection maps a system.Config field to the export document
// section that carries it. Every field must be present: either a section name,
// or the empty string with a reason recorded in exportExemptFields.
var configFieldToSection = map[string]string{
	"RotationPolicies":  "rotation_policies",
	"RetentionPolicies": "retention_policies",
	"Ingesters":         "ingesters",
	"Vaults":            "vaults",
	"Routes":            "routes",
	"Certs":             "certificates",
	"ManagedFiles":      "managed_files",
	"CloudServices":     "cloud_services",
	"Auth":              "auth",
	"Query":             "query",
	"Scheduler":         "scheduler",
	"TLS":               "tls",
	"Lookup":            "lookup",
	"Cluster":           "cluster",
	"MaxMind":           "maxmind",
	"LogLevels":         "log_levels",
}

// exportExemptFields are system.Config fields that deliberately have no export
// section. Each needs a reason here AND an entry in the document's `excluded`
// list, so an operator reading the document learns the same thing.
var exportExemptFields = map[string]string{}

func TestEveryConfigTypeHasAnExportSection(t *testing.T) {
	t.Parallel()
	cfgType := reflect.TypeOf(system.Config{})
	for i := range cfgType.NumField() {
		name := cfgType.Field(i).Name
		if !cfgType.Field(i).IsExported() {
			continue
		}
		section, mapped := configFieldToSection[name]
		if !mapped {
			if reason, exempt := exportExemptFields[name]; exempt {
				t.Logf("system.Config.%s is exempt from export: %s", name, reason)
				continue
			}
			t.Errorf("system.Config.%s has no export section and is not listed as exempt — "+
				"a config type that is neither exported nor declared in the document's "+
				"`excluded` list vanishes silently on restore (gastrolog-4j7srt)", name)
			continue
		}
		if section == "" {
			t.Errorf("system.Config.%s maps to an empty section name", name)
		}
	}
}

// protoIDSections is a SECOND hardcoded list, and it drifted the same way: a
// section whose document carries GLID strings but which is missing from that
// map imports as base64 garbage. For lookups that garbage reached
// glid.MustParse and panicked the settings endpoint. Any section backed by a
// generated proto message needs an entry.
func TestProtoSectionsDecodeTheirIDs(t *testing.T) {
	t.Parallel()
	// Sections whose document form is a generated proto message. Kept beside
	// the export doc so adding a proto-backed section forces a decision here.
	protoBacked := []string{
		"rotation_policies", "retention_policies", "cloud_services", "vaults",
		"ingesters", "routes", "node_storage_configs", "lookup",
	}
	for _, section := range protoBacked {
		if !protoIDSections[section] {
			t.Errorf("section %q decodes into a proto message but is not in protoIDSections — "+
				"its GLID strings will be read as base64 and produce garbage IDs", section)
		}
	}
}

// The section names above must actually exist in the export document, or the
// mapping is a comforting fiction.
func TestExportSectionsExistInTheDocument(t *testing.T) {
	t.Parallel()
	docType := reflect.TypeOf(exportDoc{})
	present := map[string]bool{}
	for i := range docType.NumField() {
		tag := docType.Field(i).Tag.Get("json")
		if tag == "" {
			continue
		}
		present[strings.Split(tag, ",")[0]] = true
	}
	for field, section := range configFieldToSection {
		if !present[section] {
			t.Errorf("system.Config.%s claims export section %q, but exportDoc has no such field",
				field, section)
		}
	}
}
