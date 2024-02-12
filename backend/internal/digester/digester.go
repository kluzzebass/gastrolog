package digester

type (
	// Digester interface
	Digester interface {
		Digest() error
	}
)
