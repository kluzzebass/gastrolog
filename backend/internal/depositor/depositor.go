package depositor

type (
	Depositor interface {
		Deposit(amount float64) error
	}
)
