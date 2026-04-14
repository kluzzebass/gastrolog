package cli

import (
	"gastrolog/internal/glid"
	"context"
	"fmt"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func newJobCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "job",
		Aliases: []string{"jobs"},
		Short:   "Manage jobs",
	}
	cmd.AddCommand(
		newJobListCmd(),
		newJobGetCmd(),
	)
	return cmd
}

func newJobListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Job.ListJobs(context.Background(), connect.NewRequest(&v1.ListJobsRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Jobs)
			}
			var rows [][]string
			for _, j := range resp.Msg.Jobs {
				rows = append(rows, []string{
					glid.FromBytes(j.Id).String(), j.Name, jobStatusStr(j.Status), j.Kind.String(), j.Description,
				})
			}
			p.table([]string{"ID", "NAME", "STATUS", "KIND", "DESCRIPTION"}, rows)
			return nil
		},
	}
}

func newJobGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "Get job details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Job.GetJob(context.Background(), connect.NewRequest(&v1.GetJobRequest{Id: []byte(args[0])}))
			if err != nil {
				return err
			}
			j := resp.Msg.Job
			if j == nil {
				return fmt.Errorf("job %q not found", args[0])
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(j)
			}
			pairs := [][2]string{
				{"ID", glid.FromBytes(j.Id).String()},
				{"Name", j.Name},
				{"Status", jobStatusStr(j.Status)},
				{"Kind", j.Kind.String()},
				{"Description", j.Description},
				{"Chunks", fmt.Sprintf("%d/%d", j.ChunksDone, j.ChunksTotal)},
				{"Records", strconv.FormatInt(j.RecordsDone, 10)},
			}
			if j.Error != "" {
				pairs = append(pairs, [2]string{"Error", j.Error})
			}
			if j.StartedAt != nil {
				pairs = append(pairs, [2]string{"Started", j.StartedAt.AsTime().String()})
			}
			if j.CompletedAt != nil {
				pairs = append(pairs, [2]string{"Completed", j.CompletedAt.AsTime().String()})
			}
			if j.Schedule != "" {
				pairs = append(pairs, [2]string{"Schedule", j.Schedule})
			}
			if j.NextRun != nil {
				pairs = append(pairs, [2]string{"Next Run", j.NextRun.AsTime().String()})
			}
			if len(j.NodeId) != 0 {
				pairs = append(pairs, [2]string{"Node", string(j.NodeId)})
			}
			p.kv(pairs)
			return nil
		},
	}
}

func jobStatusStr(s v1.JobStatus) string {
	return strings.TrimPrefix(s.String(), "JOB_STATUS_")
}
