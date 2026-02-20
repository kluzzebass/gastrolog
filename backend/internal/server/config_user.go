package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/auth"
)

// userPreferences is the JSON structure stored in the users.preferences column.
type userPreferences struct {
	Theme            string       `json:"theme,omitempty"`
	SyntaxHighlight  string       `json:"syntax_highlight,omitempty"`
	SavedQueries     []savedQuery `json:"saved_queries,omitempty"`
}

// savedQuery is one entry in the user's saved queries list.
type savedQuery struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

// loadPrefs reads and parses the preferences JSON for the authenticated user.
func (s *ConfigServer) loadPrefs(ctx context.Context, claims *auth.Claims) (uuid.UUID, userPreferences, error) {
	uid, err := uuid.Parse(claims.UserID)
	if err != nil {
		return uuid.Nil, userPreferences{}, fmt.Errorf("parse user id: %w", err)
	}
	raw, err := s.cfgStore.GetUserPreferences(ctx, uid)
	if err != nil {
		return uid, userPreferences{}, err
	}
	var prefs userPreferences
	if raw != nil {
		if err := json.Unmarshal([]byte(*raw), &prefs); err != nil {
			return uid, userPreferences{}, fmt.Errorf("parse preferences: %w", err)
		}
	}
	return uid, prefs, nil
}

// savePrefs marshals and writes the preferences JSON for a user.
func (s *ConfigServer) savePrefs(ctx context.Context, uid uuid.UUID, prefs userPreferences) error {
	data, err := json.Marshal(prefs)
	if err != nil {
		return err
	}
	return s.cfgStore.PutUserPreferences(ctx, uid, string(data))
}

// GetPreferences returns the current user's preferences.
func (s *ConfigServer) GetPreferences(
	ctx context.Context,
	req *connect.Request[apiv1.GetPreferencesRequest],
) (*connect.Response[apiv1.GetPreferencesResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}

	_, prefs, err := s.loadPrefs(ctx, claims)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.GetPreferencesResponse{
		Theme:           prefs.Theme,
		SyntaxHighlight: prefs.SyntaxHighlight,
	}), nil
}

// PutPreferences updates the current user's preferences.
func (s *ConfigServer) PutPreferences(
	ctx context.Context,
	req *connect.Request[apiv1.PutPreferencesRequest],
) (*connect.Response[apiv1.PutPreferencesResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}

	uid, prefs, err := s.loadPrefs(ctx, claims)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	prefs.Theme = req.Msg.Theme
	prefs.SyntaxHighlight = req.Msg.SyntaxHighlight
	if err := s.savePrefs(ctx, uid, prefs); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.PutPreferencesResponse{}), nil
}

// GetSavedQueries returns the current user's saved queries.
func (s *ConfigServer) GetSavedQueries(
	ctx context.Context,
	req *connect.Request[apiv1.GetSavedQueriesRequest],
) (*connect.Response[apiv1.GetSavedQueriesResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}

	_, prefs, err := s.loadPrefs(ctx, claims)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.GetSavedQueriesResponse{}
	for _, q := range prefs.SavedQueries {
		resp.Queries = append(resp.Queries, &apiv1.SavedQuery{
			Name:  q.Name,
			Query: q.Query,
		})
	}
	return connect.NewResponse(resp), nil
}

// PutSavedQuery creates or updates a saved query by name.
func (s *ConfigServer) PutSavedQuery(
	ctx context.Context,
	req *connect.Request[apiv1.PutSavedQueryRequest],
) (*connect.Response[apiv1.PutSavedQueryResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}
	if req.Msg.Query == nil || req.Msg.Query.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("query name required"))
	}

	uid, prefs, err := s.loadPrefs(ctx, claims)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Upsert: replace if name exists, append otherwise.
	found := false
	for i, q := range prefs.SavedQueries {
		if q.Name == req.Msg.Query.Name {
			prefs.SavedQueries[i].Query = req.Msg.Query.Query
			found = true
			break
		}
	}
	if !found {
		prefs.SavedQueries = append(prefs.SavedQueries, savedQuery{
			Name:  req.Msg.Query.Name,
			Query: req.Msg.Query.Query,
		})
	}

	if err := s.savePrefs(ctx, uid, prefs); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.PutSavedQueryResponse{}), nil
}

// DeleteSavedQuery removes a saved query by name.
func (s *ConfigServer) DeleteSavedQuery(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteSavedQueryRequest],
) (*connect.Response[apiv1.DeleteSavedQueryResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}
	if req.Msg.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("query name required"))
	}

	uid, prefs, err := s.loadPrefs(ctx, claims)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	filtered := prefs.SavedQueries[:0]
	for _, q := range prefs.SavedQueries {
		if q.Name != req.Msg.Name {
			filtered = append(filtered, q)
		}
	}
	prefs.SavedQueries = filtered

	if err := s.savePrefs(ctx, uid, prefs); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteSavedQueryResponse{}), nil
}
