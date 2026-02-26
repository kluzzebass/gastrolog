package cli

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
)

// resolver maps names to UUIDs for all entity types by calling GetConfig once.
type resolver struct {
	filters           map[string]string // name → id
	rotationPolicies  map[string]string
	retentionPolicies map[string]string
	vaults            map[string]string
	ingesters         map[string]string
	nodes             map[string]string
	users             map[string]string // username → id
	certs             map[string]string
}

// newResolver fetches the full config and user list, building name→ID maps.
func newResolver(ctx context.Context, client *server.Client) (*resolver, error) {
	resp, err := client.Config.GetConfig(ctx, connect.NewRequest(&v1.GetConfigRequest{}))
	if err != nil {
		return nil, fmt.Errorf("get config: %w", err)
	}

	r := &resolver{
		filters:           make(map[string]string),
		rotationPolicies:  make(map[string]string),
		retentionPolicies: make(map[string]string),
		vaults:            make(map[string]string),
		ingesters:         make(map[string]string),
		nodes:             make(map[string]string),
		users:             make(map[string]string),
		certs:             make(map[string]string),
	}

	cfg := resp.Msg
	for _, f := range cfg.Filters {
		r.filters[strings.ToLower(f.Name)] = f.Id
	}
	for _, p := range cfg.RotationPolicies {
		r.rotationPolicies[strings.ToLower(p.Name)] = p.Id
	}
	for _, p := range cfg.RetentionPolicies {
		r.retentionPolicies[strings.ToLower(p.Name)] = p.Id
	}
	for _, v := range cfg.Vaults {
		r.vaults[strings.ToLower(v.Name)] = v.Id
	}
	for _, i := range cfg.Ingesters {
		r.ingesters[strings.ToLower(i.Name)] = i.Id
	}
	for _, n := range cfg.NodeConfigs {
		r.nodes[strings.ToLower(n.Name)] = n.Id
	}

	// Certs via ListCertificates.
	certResp, err := client.Config.ListCertificates(ctx, connect.NewRequest(&v1.ListCertificatesRequest{}))
	if err == nil {
		for _, c := range certResp.Msg.Certificates {
			r.certs[strings.ToLower(c.Name)] = c.Id
		}
	}

	// Users via ListUsers.
	userResp, err := client.Auth.ListUsers(ctx, connect.NewRequest(&v1.ListUsersRequest{}))
	if err == nil {
		for _, u := range userResp.Msg.Users {
			r.users[strings.ToLower(u.Username)] = u.Id
		}
	}

	return r, nil
}

// resolve tries uuid.Parse first; if that fails, looks up nameOrID in the
// given map (case-insensitive). Returns the UUID string or an error.
func resolve(nameOrID string, m map[string]string, entityType string) (string, error) {
	if _, err := uuid.Parse(nameOrID); err == nil {
		return nameOrID, nil
	}
	id, ok := m[strings.ToLower(nameOrID)]
	if !ok {
		return "", fmt.Errorf("%s %q not found", entityType, nameOrID)
	}
	return id, nil
}

// parseParams converts a slice of "key=value" strings into a map.
func parseParams(kvs []string) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		k, v, _ := strings.Cut(kv, "=")
		m[k] = v
	}
	return m
}
