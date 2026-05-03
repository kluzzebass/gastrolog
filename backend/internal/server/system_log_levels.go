package server

import (
	"context"
	"errors"
	"log/slog"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// GetLogLevels returns the current default level + per-component overrides
// for this node. See gastrolog-3flfp.
func (s *SystemServer) GetLogLevels(
	_ context.Context,
	_ *connect.Request[apiv1.GetLogLevelsRequest],
) (*connect.Response[apiv1.GetLogLevelsResponse], error) {
	if s.logLevels == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("runtime log-level control not wired on this node"))
	}
	return connect.NewResponse(s.snapshotLogLevels()), nil
}

// SetLogLevel sets the minimum level for a component (or the default level
// when component is empty). See gastrolog-3flfp.
func (s *SystemServer) SetLogLevel(
	_ context.Context,
	req *connect.Request[apiv1.SetLogLevelRequest],
) (*connect.Response[apiv1.SetLogLevelResponse], error) {
	if s.logLevels == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("runtime log-level control not wired on this node"))
	}
	level, ok := protoToSlogLevel(req.Msg.GetLevel())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("level must be DEBUG, INFO, WARN, or ERROR"))
	}
	component := req.Msg.GetComponent()
	if component == "" {
		s.logLevels.SetDefaultLevel(level)
	} else {
		s.logLevels.SetLevel(component, level)
	}
	return connect.NewResponse(&apiv1.SetLogLevelResponse{
		Current: s.snapshotLogLevels(),
	}), nil
}

// ClearLogLevel removes a per-component override, falling back to the
// default level. No-op when no override is set. See gastrolog-3flfp.
func (s *SystemServer) ClearLogLevel(
	_ context.Context,
	req *connect.Request[apiv1.ClearLogLevelRequest],
) (*connect.Response[apiv1.ClearLogLevelResponse], error) {
	if s.logLevels == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("runtime log-level control not wired on this node"))
	}
	if c := req.Msg.GetComponent(); c != "" {
		s.logLevels.ClearLevel(c)
	}
	return connect.NewResponse(&apiv1.ClearLogLevelResponse{
		Current: s.snapshotLogLevels(),
	}), nil
}

// snapshotLogLevels builds a GetLogLevelsResponse from the filter handler's
// current state. Caller must have verified s.logLevels != nil.
func (s *SystemServer) snapshotLogLevels() *apiv1.GetLogLevelsResponse {
	overrides := s.logLevels.Overrides()
	resp := &apiv1.GetLogLevelsResponse{
		DefaultLevel: slogToProtoLevel(s.logLevels.DefaultLevel()),
	}
	for component, level := range overrides {
		resp.Overrides = append(resp.Overrides, &apiv1.LogLevelEntry{
			Component: component,
			Level:     slogToProtoLevel(level),
		})
	}
	return resp
}

func slogToProtoLevel(l slog.Level) apiv1.LogLevel {
	switch {
	case l <= slog.LevelDebug:
		return apiv1.LogLevel_LOG_LEVEL_DEBUG
	case l <= slog.LevelInfo:
		return apiv1.LogLevel_LOG_LEVEL_INFO
	case l <= slog.LevelWarn:
		return apiv1.LogLevel_LOG_LEVEL_WARN
	default:
		return apiv1.LogLevel_LOG_LEVEL_ERROR
	}
}

func protoToSlogLevel(l apiv1.LogLevel) (slog.Level, bool) {
	switch l {
	case apiv1.LogLevel_LOG_LEVEL_DEBUG:
		return slog.LevelDebug, true
	case apiv1.LogLevel_LOG_LEVEL_INFO:
		return slog.LevelInfo, true
	case apiv1.LogLevel_LOG_LEVEL_WARN:
		return slog.LevelWarn, true
	case apiv1.LogLevel_LOG_LEVEL_ERROR:
		return slog.LevelError, true
	case apiv1.LogLevel_LOG_LEVEL_UNSPECIFIED:
		return 0, false
	default:
		return 0, false
	}
}
