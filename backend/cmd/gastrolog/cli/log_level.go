package cli

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// newLogLevelCmd returns the "config log-level" subcommand group for
// cluster-wide per-component log-level management (gastrolog-3flfp).
func newLogLevelCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log-level",
		Short: "Manage cluster-wide per-component log levels",
		Long: "Per-component log levels are stored in the system config and " +
			"propagate to every node via Raft. Patterns follow gitignore-style globs: " +
			"\"orchestrator.replication\" exact match, \"orchestrator.*\" one segment " +
			"below, \"orchestrator.**\" any depth. Levels: debug, info, warn, error.",
	}
	cmd.AddCommand(
		newLogLevelListCmd(),
		newLogLevelSetCmd(),
		newLogLevelClearCmd(),
		newLogLevelResetCmd(),
		newLogLevelComponentsCmd(),
	)
	return cmd
}

func newLogLevelListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "Show the current default and pattern overrides",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			cfg := resp.Msg.GetLogLevels()
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(cfg)
			}
			defaultLevel := levelEnumLabel(cfg.GetDefaultLevel())
			if defaultLevel == "" {
				defaultLevel = "info" // proto UNSPECIFIED maps to INFO at runtime
			}
			rows := [][]string{{"default", defaultLevel}}
			for _, r := range cfg.GetRules() {
				rows = append(rows, []string{r.GetPattern(), levelEnumLabel(r.GetLevel())})
			}
			p.table([]string{"PATTERN", "LEVEL"}, rows)
			return nil
		},
	}
}

func newLogLevelSetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set <pattern>=<level> [<pattern>=<level>...]",
		Short: "Set one or more rules (or the default with pattern=default)",
		Long: "Set one or more rules. Each argument is a pattern=level pair (e.g. " +
			"\"orchestrator.replication=debug\"). Use \"default=<level>\" to change " +
			"the fallback. Rules with the same pattern replace the existing rule; " +
			"rules with new patterns are added. Levels: debug, info, warn, error.",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			cfg := resp.Msg.GetLogLevels()
			if cfg == nil {
				cfg = &v1.LogLevelConfig{}
			}
			for _, a := range args {
				key, val, ok := strings.Cut(a, "=")
				if !ok {
					return fmt.Errorf("invalid argument %q (want pattern=level)", a)
				}
				key = strings.TrimSpace(key)
				lvl, err := parseLevelLabel(strings.TrimSpace(val))
				if err != nil {
					return fmt.Errorf("argument %q: %w", a, err)
				}
				if key == "default" {
					cfg.DefaultLevel = lvl
					continue
				}
				cfg.Rules = upsertRule(cfg.Rules, key, lvl)
			}
			_, err = client.System.PutLogLevels(ctx, connect.NewRequest(&v1.PutLogLevelsRequest{Config: cfg}))
			return err
		},
	}
}

func newLogLevelClearCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clear <pattern> [<pattern>...]",
		Short: "Remove one or more rules (the default stays)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			cfg := resp.Msg.GetLogLevels()
			if cfg == nil {
				cfg = &v1.LogLevelConfig{}
			}
			toClear := make(map[string]bool, len(args))
			for _, p := range args {
				toClear[strings.TrimSpace(p)] = true
			}
			filtered := cfg.Rules[:0]
			for _, r := range cfg.Rules {
				if !toClear[r.GetPattern()] {
					filtered = append(filtered, r)
				}
			}
			cfg.Rules = filtered
			_, err = client.System.PutLogLevels(ctx, connect.NewRequest(&v1.PutLogLevelsRequest{Config: cfg}))
			return err
		},
	}
}

func newLogLevelResetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reset",
		Short: "Remove every rule and reset the default to info",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := context.Background()
			client := clientFromCmd(cmd)
			_, err := client.System.PutLogLevels(ctx, connect.NewRequest(&v1.PutLogLevelsRequest{
				Config: &v1.LogLevelConfig{DefaultLevel: v1.LogLevel_LOG_LEVEL_INFO},
			}))
			return err
		},
	}
}

func newLogLevelComponentsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "components",
		Short: "List every component path the binary registers, with its effective level",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := context.Background()
			client := clientFromCmd(cmd)
			resp, err := client.System.ListLogComponents(ctx, connect.NewRequest(&v1.ListLogComponentsRequest{}))
			if err != nil {
				return err
			}
			prefix, _ := cmd.Flags().GetString("prefix")
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.GetComponents())
			}
			rows := [][]string{}
			for _, c := range resp.Msg.GetComponents() {
				if prefix != "" && !strings.HasPrefix(c.GetPath(), prefix) {
					continue
				}
				rows = append(rows, []string{
					c.GetPath(),
					levelEnumLabel(c.GetEffectiveLevel()),
					sourceLabel(c.GetSource()),
				})
			}
			p.table([]string{"PATH", "EFFECTIVE", "SOURCE"}, rows)
			return nil
		},
	}
	cmd.Flags().String("prefix", "", "only list components whose path starts with this prefix")
	return cmd
}

// upsertRule replaces a rule with the same pattern, or appends if new.
func upsertRule(rules []*v1.LogLevelRule, pattern string, level v1.LogLevel) []*v1.LogLevelRule {
	for i, r := range rules {
		if r.GetPattern() == pattern {
			rules[i] = &v1.LogLevelRule{Pattern: pattern, Level: level}
			return rules
		}
	}
	return append(slices.Clone(rules), &v1.LogLevelRule{Pattern: pattern, Level: level})
}

func parseLevelLabel(s string) (v1.LogLevel, error) {
	switch strings.ToLower(s) {
	case "debug":
		return v1.LogLevel_LOG_LEVEL_DEBUG, nil
	case "info":
		return v1.LogLevel_LOG_LEVEL_INFO, nil
	case "warn", "warning":
		return v1.LogLevel_LOG_LEVEL_WARN, nil
	case "error":
		return v1.LogLevel_LOG_LEVEL_ERROR, nil
	default:
		return v1.LogLevel_LOG_LEVEL_UNSPECIFIED, fmt.Errorf("unknown level %q (allowed: debug, info, warn, error)", s)
	}
}

func levelEnumLabel(l v1.LogLevel) string {
	switch l {
	case v1.LogLevel_LOG_LEVEL_DEBUG:
		return "debug"
	case v1.LogLevel_LOG_LEVEL_INFO:
		return "info"
	case v1.LogLevel_LOG_LEVEL_WARN:
		return "warn"
	case v1.LogLevel_LOG_LEVEL_ERROR:
		return "error"
	case v1.LogLevel_LOG_LEVEL_UNSPECIFIED:
		return ""
	default:
		return ""
	}
}

func sourceLabel(s v1.LogComponentLevelSource) string {
	switch s {
	case v1.LogComponentLevelSource_LOG_LEVEL_SOURCE_DEFAULT:
		return "default"
	case v1.LogComponentLevelSource_LOG_LEVEL_SOURCE_EXACT_RULE:
		return "exact"
	case v1.LogComponentLevelSource_LOG_LEVEL_SOURCE_GLOB_RULE:
		return "glob"
	case v1.LogComponentLevelSource_LOG_LEVEL_SOURCE_UNSPECIFIED:
		return ""
	default:
		return ""
	}
}
