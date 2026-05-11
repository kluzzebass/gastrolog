package server

import (
	"context"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// Phase 2a (gastrolog-3flfp) ships the wire format only. Phase 2b wires
// the config store + FSM dispatcher hook and replaces these stubs with
// real implementations that build a logging.RuleSet from LogLevelConfig
// and call ComponentFilterHandler.SetRuleSet on every node via the
// configSignal broadcast.

// PutLogLevels is a Phase 2a stub. See system_log_levels.go.
func (s *SystemServer) PutLogLevels(
	_ context.Context,
	_ *connect.Request[apiv1.PutLogLevelsRequest],
) (*connect.Response[apiv1.PutLogLevelsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}

// ListLogComponents is a Phase 2a stub. Phase 2b returns the comp.All()
// set with each path's effective level resolved against the current
// RuleSet on the receiving node.
func (s *SystemServer) ListLogComponents(
	_ context.Context,
	_ *connect.Request[apiv1.ListLogComponentsRequest],
) (*connect.Response[apiv1.ListLogComponentsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
