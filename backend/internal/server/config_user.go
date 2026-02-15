package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/auth"
)

// userPreferences is the JSON structure stored per user.
type userPreferences struct {
	Theme string `json:"theme,omitempty"`
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

	key := "user:" + claims.UserID + ":prefs"
	raw, err := s.cfgStore.GetSetting(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.GetPreferencesResponse{}
	if raw != nil {
		var prefs userPreferences
		if err := json.Unmarshal([]byte(*raw), &prefs); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse preferences: %w", err))
		}
		resp.Theme = prefs.Theme
	}

	return connect.NewResponse(resp), nil
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

	prefs := userPreferences{
		Theme: req.Msg.Theme,
	}
	data, err := json.Marshal(prefs)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	key := "user:" + claims.UserID + ":prefs"
	if err := s.cfgStore.PutSetting(ctx, key, string(data)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.PutPreferencesResponse{}), nil
}

// savedQuery is the JSON structure for a single saved query.
type savedQuery struct {
	Name  string `json:"name"`
	Query string `json:"query"`
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

	key := "user:" + claims.UserID + ":saved_queries"
	raw, err := s.cfgStore.GetSetting(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.GetSavedQueriesResponse{}
	if raw != nil {
		var queries []savedQuery
		if err := json.Unmarshal([]byte(*raw), &queries); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse saved queries: %w", err))
		}
		for _, q := range queries {
			resp.Queries = append(resp.Queries, &apiv1.SavedQuery{
				Name:  q.Name,
				Query: q.Query,
			})
		}
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

	key := "user:" + claims.UserID + ":saved_queries"
	raw, err := s.cfgStore.GetSetting(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var queries []savedQuery
	if raw != nil {
		if err := json.Unmarshal([]byte(*raw), &queries); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse saved queries: %w", err))
		}
	}

	// Upsert: replace if name exists, append otherwise.
	found := false
	for i, q := range queries {
		if q.Name == req.Msg.Query.Name {
			queries[i].Query = req.Msg.Query.Query
			found = true
			break
		}
	}
	if !found {
		queries = append(queries, savedQuery{
			Name:  req.Msg.Query.Name,
			Query: req.Msg.Query.Query,
		})
	}

	data, err := json.Marshal(queries)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := s.cfgStore.PutSetting(ctx, key, string(data)); err != nil {
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

	key := "user:" + claims.UserID + ":saved_queries"
	raw, err := s.cfgStore.GetSetting(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var queries []savedQuery
	if raw != nil {
		if err := json.Unmarshal([]byte(*raw), &queries); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse saved queries: %w", err))
		}
	}

	filtered := queries[:0]
	for _, q := range queries {
		if q.Name != req.Msg.Name {
			filtered = append(filtered, q)
		}
	}

	if len(filtered) == 0 {
		if err := s.cfgStore.DeleteSetting(ctx, key); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	} else {
		data, err := json.Marshal(filtered)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if err := s.cfgStore.PutSetting(ctx, key, string(data)); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	return connect.NewResponse(&apiv1.DeleteSavedQueryResponse{}), nil
}
