package app

import (
	"context"
	"log/slog"

	"gastrolog/internal/logging"
	"gastrolog/internal/notify"
	"gastrolog/internal/system"
)

// WatchLogLevels keeps a ComponentFilterHandler's rule set in sync with
// the cluster-wide LogLevelConfig stored in the system config store
// (gastrolog-3flfp).
//
// On startup it reads the current LogLevelConfig and applies it once,
// then blocks on the config signal: every time the FSM dispatches a
// config change (including LogLevels-targeted mutations), it re-reads
// the config and atomically swaps the filter's rule set.
//
// The cost of waking up on unrelated config changes (vault adds, route
// edits) is one store read + one RuleSet construction — cheap compared
// to the alternative of a dedicated signal channel, and avoids
// inventing a separate broadcast for one config field.
//
// The function blocks until ctx is cancelled.
func WatchLogLevels(ctx context.Context, filter *logging.ComponentFilterHandler, store system.Store, signal *notify.Signal, logger *slog.Logger) {
	if filter == nil || store == nil || signal == nil {
		return
	}
	applyLogLevels(ctx, filter, store, logger)
	for {
		ch := signal.C()
		select {
		case <-ctx.Done():
			return
		case <-ch:
			applyLogLevels(ctx, filter, store, logger)
		}
	}
}

func applyLogLevels(ctx context.Context, filter *logging.ComponentFilterHandler, store system.Store, logger *slog.Logger) {
	cfg, err := store.GetLogLevels(ctx)
	if err != nil {
		if logger != nil {
			logger.Warn("log-level watcher: read failed", "err", err)
		}
		return
	}
	filter.SetRuleSet(buildLogLevelRuleSet(cfg))
}

// buildLogLevelRuleSet adapts a system.LogLevelConfig to a logging.RuleSet,
// drawing a fresh generation so derived filter handlers invalidate their
// resolution cache.
func buildLogLevelRuleSet(cfg system.LogLevelConfig) logging.RuleSet {
	rules := make([]logging.LevelRule, len(cfg.Rules))
	for i, r := range cfg.Rules {
		rules[i] = logging.LevelRule{
			Pattern: r.Pattern,
			Level:   slog.Level(r.Level),
		}
	}
	return logging.NewRuleSet(slog.Level(cfg.Default), rules, logging.NextGeneration())
}
