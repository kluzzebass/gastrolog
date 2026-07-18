package server

// Alarm shelving RPCs: ShelveAlarm, UnshelveAlarm. Cluster-first: alarms
// are raised per-node and aggregated via the PeerState broadcast, so the
// node serving one of these RPCs is usually NOT the node whose collector
// holds the alarm. The serving node resolves every raiser of the alarm ID
// — its own collector plus each peer whose broadcast NodeStats carries the
// ID — applies locally where applicable, and forwards a local_only leg to
// every remote raiser via ForwardRPC. A cluster-wide condition raised by
// multiple nodes (e.g. vault-leaderless on every orchestrator) is
// therefore shelved everywhere in one call. Forward failures surface as
// errors naming the unreachable nodes — never silently dropped; the
// operations are idempotent, so the operator retries.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/alert"
	"gastrolog/internal/auth"
	"gastrolog/internal/server/routing"
)

// SetAlarmLifecycle wires the local alarm collector and the cluster
// forwarder into the lifecycle server. alerts nil disables the alarm
// shelving RPCs; forwarder nil limits them to locally-raised alarms
// (single-node mode).
func (s *LifecycleServer) SetAlarmLifecycle(alerts *alert.Collector, forwarder routing.UnaryForwarder) {
	s.alerts = alerts
	s.alarmForwarder = forwarder
}

// alarmLifecycleErr maps collector shelving errors to Connect codes.
func alarmLifecycleErr(err error) error {
	switch {
	case errors.Is(err, alert.ErrUnknownAlarm):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, alert.ErrShelveExpiryRequired):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, alert.ErrNotShelveable),
		errors.Is(err, alert.ErrNotShelved):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	default:
		return errInternal(err)
	}
}

// operatorIdentity resolves who to record on a shelve. The explicit
// request field wins: the Unix-socket CLI supplies the OS username (its
// no-auth context only carries the synthetic "admin" claims), and forwarded
// fan-out legs carry the identity already resolved by the serving node —
// the remote's NoAuth interceptor must not overwrite it. Absent that, the
// authenticated user's name; the record must never be blank.
func operatorIdentity(ctx context.Context, fromRequest string) string {
	if fromRequest != "" {
		return fromRequest
	}
	if claims := auth.ClaimsFromContext(ctx); claims != nil {
		if u := claims.Username(); u != "" {
			return u
		}
	}
	return "operator"
}

// alarmRaisers resolves which nodes currently raise the given alarm ID:
// the local collector and every peer whose latest broadcast NodeStats
// carries it (in any state — a shelve must reach a raiser whose alarm is
// already shelved so the expiry refreshes everywhere).
func (s *LifecycleServer) alarmRaisers(alarmID string) (local bool, remote []string) {
	if s.alerts != nil && s.alerts.HasStanding(alarmID) {
		local = true
	}
	if s.cluster == nil || s.peerStats == nil {
		return local, nil
	}
	servers, err := s.cluster.Servers()
	if err != nil {
		s.logger.Warn("alarm lifecycle: list cluster servers", "error", err)
		return local, nil
	}
	for _, srv := range servers {
		if srv.ID == s.nodeID {
			continue
		}
		stats := s.peerStats.Get(srv.ID)
		if stats == nil {
			continue
		}
		for _, a := range stats.Alerts {
			if string(a.Id) == alarmID {
				remote = append(remote, srv.ID)
				break
			}
		}
	}
	return local, remote
}

// alarmForwardTimeout caps each remote fan-out leg. Same rationale as
// peerInspectorTimeout: comfortably above a healthy round-trip, well below
// operator patience; an unreachable raiser surfaces as an error (never
// silently elided — a half-applied shelve that looks whole would leave the
// alarm standing on the missed node).
const alarmForwardTimeout = 3 * time.Second

// fanOutAlarmOp applies an alarm lifecycle operation cluster-wide: local
// collector first (when it raises the ID), then a local_only forward to
// every remote raiser. Returns how many raisers applied and an error
// naming every node that failed — partial application is reported, not
// hidden, and the operations are idempotent so retrying is safe.
func (s *LifecycleServer) fanOutAlarmOp(
	ctx context.Context,
	op string,
	alarmID string,
	applyLocal func() error,
	forward func(ctx context.Context, nodeID string) error,
) (applied uint32, err error) {
	local, remote := s.alarmRaisers(alarmID)
	if !local && len(remote) == 0 {
		return 0, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("no standing alarm %q on any cluster node", alarmID))
	}
	var failures []string
	if local {
		if lerr := applyLocal(); lerr != nil {
			// The collector settles state on every read; the alarm can
			// legitimately release between discovery and apply. Anything
			// else is a real failure.
			if !errors.Is(lerr, alert.ErrUnknownAlarm) || len(remote) == 0 {
				return 0, alarmLifecycleErr(lerr)
			}
		} else {
			applied++
		}
	}
	for _, nodeID := range remote {
		if s.alarmForwarder == nil {
			failures = append(failures, nodeID+" (no cluster forwarder)")
			continue
		}
		fctx, cancel := context.WithTimeout(ctx, alarmForwardTimeout)
		ferr := forward(fctx, nodeID)
		cancel()
		if ferr != nil {
			s.logger.Warn("alarm lifecycle: forward to raiser failed",
				"op", op, "alarm", alarmID, "node", nodeID, "error", ferr)
			failures = append(failures, fmt.Sprintf("%s (%v)", nodeID, ferr))
			continue
		}
		applied++
	}
	if len(failures) > 0 {
		return applied, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("%s of alarm %q applied on %d raiser(s) but failed on: %s — retry once the node is reachable",
				op, alarmID, applied, strings.Join(failures, ", ")))
	}
	return applied, nil
}

// ShelveAlarm shelves a standing alarm on every node that raises it. The
// mandatory-expiry and shelveability gates run here at the API boundary —
// before any fan-out — and again inside each collector.
func (s *LifecycleServer) ShelveAlarm(
	ctx context.Context,
	req *connect.Request[apiv1.ShelveAlarmRequest],
) (*connect.Response[apiv1.ShelveAlarmResponse], error) {
	if s.alerts == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("alarm lifecycle not available on this node"))
	}
	alarmID := string(req.Msg.AlarmId)
	if alarmID == "" {
		return nil, errRequired("alarm_id")
	}
	if req.Msg.DurationSeconds <= 0 {
		return nil, alarmLifecycleErr(alert.ErrShelveExpiryRequired)
	}
	d := time.Duration(req.Msg.DurationSeconds) * time.Second
	// Catalog gate: types where deferral is meaningless refuse shelve
	// before any state is touched. The collector re-checks per entry
	// (authoritative for operator-defined and unregistered types).
	typeID, _, _ := strings.Cut(alarmID, ":")
	if t, ok := alert.TypeByID(typeID); ok && !t.Shelveable() {
		return nil, alarmLifecycleErr(fmt.Errorf("%w: %s", alert.ErrNotShelveable, typeID))
	}
	by := operatorIdentity(ctx, req.Msg.ShelvedBy)

	if req.Msg.LocalOnly {
		until, err := s.alerts.Shelve(alarmID, d, by)
		if err != nil {
			return nil, alarmLifecycleErr(err)
		}
		return connect.NewResponse(&apiv1.ShelveAlarmResponse{Applied: 1, ShelvedUntil: timestamppb.New(until)}), nil
	}

	var until time.Time
	applied, err := s.fanOutAlarmOp(ctx, "shelve", alarmID,
		func() error {
			u, serr := s.alerts.Shelve(alarmID, d, by)
			if serr == nil && until.IsZero() {
				until = u
			}
			return serr
		},
		func(fctx context.Context, nodeID string) error {
			resp := &apiv1.ShelveAlarmResponse{}
			ferr := s.forwardAlarmOp(fctx, nodeID, gastrologv1connect.LifecycleServiceShelveAlarmProcedure,
				&apiv1.ShelveAlarmRequest{AlarmId: req.Msg.AlarmId, DurationSeconds: req.Msg.DurationSeconds, ShelvedBy: by, LocalOnly: true}, resp)
			if ferr == nil && until.IsZero() && resp.ShelvedUntil != nil {
				until = resp.ShelvedUntil.AsTime()
			}
			return ferr
		})
	if err != nil {
		return nil, err
	}
	out := &apiv1.ShelveAlarmResponse{Applied: applied}
	if !until.IsZero() {
		out.ShelvedUntil = timestamppb.New(until)
	}
	return connect.NewResponse(out), nil
}

// UnshelveAlarm ends a shelve early on every node that raises the alarm.
func (s *LifecycleServer) UnshelveAlarm(
	ctx context.Context,
	req *connect.Request[apiv1.UnshelveAlarmRequest],
) (*connect.Response[apiv1.UnshelveAlarmResponse], error) {
	if s.alerts == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("alarm lifecycle not available on this node"))
	}
	alarmID := string(req.Msg.AlarmId)
	if alarmID == "" {
		return nil, errRequired("alarm_id")
	}

	if req.Msg.LocalOnly {
		if err := s.alerts.Unshelve(alarmID); err != nil {
			return nil, alarmLifecycleErr(err)
		}
		return connect.NewResponse(&apiv1.UnshelveAlarmResponse{Applied: 1}), nil
	}

	applied, err := s.fanOutAlarmOp(ctx, "unshelve", alarmID,
		func() error { return s.alerts.Unshelve(alarmID) },
		func(fctx context.Context, nodeID string) error {
			return s.forwardAlarmOp(fctx, nodeID, gastrologv1connect.LifecycleServiceUnshelveAlarmProcedure,
				&apiv1.UnshelveAlarmRequest{AlarmId: req.Msg.AlarmId, LocalOnly: true}, &apiv1.UnshelveAlarmResponse{})
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.UnshelveAlarmResponse{Applied: applied}), nil
}

// forwardAlarmOp sends one local_only lifecycle request to a remote raiser
// over ForwardRPC and unmarshals the response into resp.
func (s *LifecycleServer) forwardAlarmOp(ctx context.Context, nodeID, procedure string, req, resp proto.Message) error {
	payload, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", procedure, err)
	}
	respPayload, err := s.alarmForwarder.ForwardUnary(ctx, nodeID, procedure, payload)
	if err != nil {
		return err
	}
	return proto.Unmarshal(respPayload, resp)
}
