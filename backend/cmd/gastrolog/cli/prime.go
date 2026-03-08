package cli

import (
	_ "embed"
	"fmt"

	"github.com/spf13/cobra"
)

//go:embed prime.md
var primeText string

// NewPrimeCommand returns the "prime" command that prints AI agent guidance.
func NewPrimeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "prime",
		Short: "Print logging guidance for AI agents",
		Long:  "Prints structured guidance on how to configure logging for applications that send logs to gastrolog. Pipe this into your agent's context.",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Print(primeText)
		},
	}
}
