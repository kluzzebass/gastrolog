//go:build ignore

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go run scripts/probe_backlog.go <home> [vault-name]")
		os.Exit(2)
	}
	hd := home.New(os.Args[1])
	vaultName := "first-vault"
	if len(os.Args) > 2 {
		vaultName = os.Args[2]
	}
	httpClient := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return net.Dial("unix", hd.SocketPath())
	}}}
	client := server.NewClientWithHTTP(httpClient, "http://localhost")
	ctx := context.Background()
	sys, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
	if err != nil {
		panic(err)
	}
	var vaultID string
	for _, v := range sys.Msg.Vaults {
		id := glid.FromBytes(v.Id).String()
		if v.Name == vaultName || id == vaultName {
			vaultID = id
			break
		}
	}
	resp, err := client.Vault.GetPipelineBacklog(ctx, connect.NewRequest(&v1.GetPipelineBacklogRequest{Vault: vaultID}))
	if err != nil {
		panic(err)
	}
	b := resp.Msg.Backlog
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(b)
	if b.OldestEligibleLastIngest != nil {
		ts := time.Unix(b.OldestEligibleLastIngest.Seconds, int64(b.OldestEligibleLastIngest.Nanos)).UTC()
		fmt.Fprintf(os.Stderr, "oldest_eligible_ingest=%s lag=%s\n", ts, time.Since(ts))
	}
}
