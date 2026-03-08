package cli

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func newFileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "file",
		Aliases: []string{"files"},
		Short:   "Manage files",
	}
	cmd.AddCommand(
		newFileListCmd(),
		newFileUploadCmd(),
		newFileDeleteCmd(),
	)
	return cmd
}

func newFileListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all managed files",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.ListManagedFiles(context.Background(), connect.NewRequest(&v1.ListManagedFilesRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Files)
			}
			var rows [][]string
			for _, f := range resp.Msg.Files {
				rows = append(rows, []string{
					f.Id, f.Name, formatBytesStr(f.Size), f.Sha256[:12], f.UploadedAt,
				})
			}
			p.table([]string{"ID", "NAME", "SIZE", "SHA256", "UPLOADED"}, rows)
			return nil
		},
	}
}

func newFileUploadCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "upload <path>",
		Short: "Upload a file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath := args[0]

			f, err := os.Open(filePath) //nolint:gosec // user-specified path is the whole point
			if err != nil {
				return fmt.Errorf("open %s: %w", filePath, err)
			}
			defer f.Close() //nolint:errcheck

			// Build multipart body.
			pr, pw := io.Pipe()
			writer := multipart.NewWriter(pw)
			go func() {
				part, err := writer.CreateFormFile("file", filepath.Base(filePath))
				if err != nil {
					_ = pw.CloseWithError(err)
					return
				}
				if _, err := io.Copy(part, f); err != nil {
					_ = pw.CloseWithError(err)
					return
				}
				_ = pw.CloseWithError(writer.Close())
			}()

			addr, _ := cmd.Flags().GetString("addr")
			token, _ := cmd.Flags().GetString("token")
			if token == "" {
				token = envToken()
			}

			url := addr + "/api/v1/managed-files/upload"
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, pr)
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", writer.FormDataContentType())
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}

			resp, err := http.DefaultClient.Do(req) //nolint:gosec // user-specified addr is the whole point
			if err != nil {
				return err
			}
			defer resp.Body.Close() //nolint:errcheck
			if resp.StatusCode != http.StatusCreated {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("upload failed (%d): %s", resp.StatusCode, string(body))
			}

			fmt.Printf("Uploaded %s\n", filepath.Base(filePath))
			return nil
		},
	}
}

func newFileDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a managed file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			_, err := client.Config.DeleteManagedFile(context.Background(), connect.NewRequest(&v1.DeleteManagedFileRequest{Id: args[0]}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted file %s\n", args[0])
			return nil
		},
	}
}

func formatBytesStr(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(bytes)/(1024*1024))
}
