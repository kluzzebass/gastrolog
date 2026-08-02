package glcb

import (
	"errors"
	"fmt"

	"gastrolog/internal/tsindex"
)

// ErrNoSectionCodec is returned when a TOC entry names a (type, version) no
// registered codec handles. Callers treat it like an unreadable section —
// skip, estimate, or surface — never as fatal: a newer node may have written
// a section version this binary does not know yet.
var ErrNoSectionCodec = errors.New("glcb: no codec registered for section")

// SectionCodec decodes one version of one TOC section kind. A codec is pure
// dispatch state: it holds no bytes and no I/O, it only knows how to wrap a
// section's raw payload in the section kind's typed view.
//
// Views are typed per section KIND, not per version — every version of the
// TS-index sections yields a tsindex.View — so readers depend on the kind's
// semantic API and never on a layout. Callers narrow the returned any once,
// at the read seam (the Bluge/Ice segment-plugin shape; a generic interface
// cannot live in one registry map).
type SectionCodec interface {
	SectionType() byte
	SectionVersion() uint8
	// NewView wraps raw payload bytes in the section kind's typed view. The
	// bytes' lifetime belongs to the caller (an mmap alias or a heap buffer);
	// views read through them and must not be used past it.
	NewView(data []byte) (any, error)
}

// Registry maps (section type, section version) to the codec that decodes
// it. Immutable after construction: the codec set is a compile-time fact of
// the binary, and a mutable global would make decode behavior depend on
// wiring order.
type Registry struct {
	codecs map[registryKey]SectionCodec
}

type registryKey struct {
	sectionType byte
	version     uint8
}

// NewRegistry builds a registry from the given codecs, rejecting duplicate
// (type, version) claims — last-writer-wins would make decode behavior
// depend on registration order.
func NewRegistry(codecs ...SectionCodec) (*Registry, error) {
	m := make(map[registryKey]SectionCodec, len(codecs))
	for _, c := range codecs {
		k := registryKey{sectionType: c.SectionType(), version: c.SectionVersion()}
		if _, dup := m[k]; dup {
			return nil, fmt.Errorf("glcb: duplicate codec for section type 0x%02x version %d", k.sectionType, k.version)
		}
		m[k] = c
	}
	return &Registry{codecs: m}, nil
}

// NewView dispatches on the TOC entry's (type, version) and wraps data in
// the matching codec's view.
func (r *Registry) NewView(entry TOCEntry, data []byte) (any, error) {
	c, ok := r.codecs[registryKey{sectionType: entry.Type, version: entry.Version}]
	if !ok {
		return nil, fmt.Errorf("%w: type 0x%02x version %d", ErrNoSectionCodec, entry.Type, entry.Version)
	}
	return c.NewView(data)
}

// tsIndexSectionVersion is the version the writer stamps on the ITSI/STSI
// sections it emits and the version tsIndexCodec claims — one constant so
// write and read cannot drift.
const tsIndexSectionVersion = 1

// tsIndexCodec is the version-1 TS-index section codec: raw contiguous
// [ts:i64][pos:u32] entries, TS-sorted, no header. One instance per section
// type byte; the layout is identical for ITSI and STSI.
type tsIndexCodec struct {
	sectionType byte
}

func (c tsIndexCodec) SectionType() byte     { return c.sectionType }
func (c tsIndexCodec) SectionVersion() uint8 { return tsIndexSectionVersion }

func (c tsIndexCodec) NewView(data []byte) (any, error) {
	v, err := tsindex.NewRawView(data)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// BuiltinSectionCodecs returns the codecs this binary ships: the version-1
// ITSI and STSI TS-index codecs. Exposed so tests (and future callers) can
// compose them with additional codecs into a custom Registry.
func BuiltinSectionCodecs() []SectionCodec {
	return []SectionCodec{
		tsIndexCodec{sectionType: SectionIngestTSIndex},
		tsIndexCodec{sectionType: SectionSourceTSIndex},
	}
}

// defaultRegistry is built once from the builtin codecs; BuiltinSectionCodecs
// never errors on itself (distinct keys by construction).
var defaultRegistry = func() *Registry {
	r, err := NewRegistry(BuiltinSectionCodecs()...)
	if err != nil {
		panic(err) // unreachable: builtin keys are distinct by construction
	}
	return r
}()

// DefaultRegistry returns the process-wide registry of builtin section
// codecs. Readers that need extra codecs compose their own via NewRegistry.
func DefaultRegistry() *Registry { return defaultRegistry }
