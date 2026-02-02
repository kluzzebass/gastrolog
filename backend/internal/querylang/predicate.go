package querylang

// PredicateKind identifies the type of leaf predicate.
type PredicateKind int

const (
	// PredToken represents a bare word token search: "error"
	PredToken PredicateKind = iota

	// PredKV represents an exact key=value match: "level=error"
	PredKV

	// PredKeyExists represents a key existence check: "level=*"
	PredKeyExists

	// PredValueExists represents a value existence check: "*=error"
	PredValueExists
)

func (k PredicateKind) String() string {
	switch k {
	case PredToken:
		return "token"
	case PredKV:
		return "kv"
	case PredKeyExists:
		return "key_exists"
	case PredValueExists:
		return "value_exists"
	default:
		return "unknown"
	}
}
