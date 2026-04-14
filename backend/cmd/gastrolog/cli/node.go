package cli

import (
	"gastrolog/internal/glid"
	"context"
	"fmt"
	"strconv"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func newNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Manage node configurations",
	}
	cmd.AddCommand(
		newNodeListCmd(),
		newNodeGetCmd(),
		newNodeRenameCmd(),
		newNodeAddStorageCmd(),
		newNodeListStorageCmd(),
	)
	return cmd
}

func newNodeListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.NodeConfigs)
			}
			var rows [][]string
			for _, n := range resp.Msg.NodeConfigs {
				rows = append(rows, []string{glid.FromBytes(n.Id).String(), n.Name})
			}
			p.table([]string{"ID", "NAME"}, rows)
			return nil
		},
	}
}

func newNodeGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get node details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.nodes, "node")
			if err != nil {
				return err
			}
			idBytes, parseErr := glid.ParseUUID(id)
			if parseErr != nil {
				return parseErr
			}
			for _, n := range resp.Msg.NodeConfigs {
				if glid.FromBytes(n.Id) == idBytes {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(n)
					}
					p.kv([][2]string{
						{"ID", glid.FromBytes(n.Id).String()},
						{"Name", n.Name},
					})
					return nil
				}
			}
			return fmt.Errorf("node %q not found", args[0])
		},
	}
}

func newNodeRenameCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rename <name-or-id> <new-name>",
		Short: "Rename a node",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.nodes, "node")
			if err != nil {
				return err
			}
			idBytes, parseErr := glid.ParseUUID(id)
			if parseErr != nil {
				return parseErr
			}
			_, err = client.System.PutNodeConfig(context.Background(), connect.NewRequest(&v1.PutNodeConfigRequest{
				Config: &v1.NodeConfig{
					Id:   idBytes.ToProto(),
					Name: args[1],
				},
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Renamed node %s to %q\n", args[0], args[1])
			return nil
		},
	}
	return cmd
}

func newNodeAddStorageCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add-storage <node-name-or-id>",
		Short: "Add a file storage to a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			path, _ := cmd.Flags().GetString("path")
			storageClass, _ := cmd.Flags().GetUint32("storage-class")

			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			nodeID, err := resolve(args[0], r.nodes, "node")
			if err != nil {
				return err
			}

			// Get existing storage config for this node.
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			var existing []*v1.FileStorage
			for _, nsc := range resp.Msg.NodeStorageConfigs {
				if glid.FromBytes(nsc.NodeId).String() == nodeID {
					existing = nsc.FileStorages
					break
				}
			}

			// Append new storage.
			newFsID := glid.New()
			existing = append(existing, &v1.FileStorage{
				Id:           newFsID.ToProto(),
				Name:         name,
				Path:         path,
				StorageClass: storageClass,
			})

			_, err = client.System.SetNodeStorageConfig(context.Background(), connect.NewRequest(&v1.SetNodeStorageConfigRequest{
				Config: &v1.NodeStorageConfig{
					NodeId:       []byte(nodeID),
					FileStorages: existing,
				},
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Added file storage %q to node %s (id=%s, class=%d, path=%s)\n", name, args[0], newFsID, storageClass, path)
			return nil
		},
	}
	cmd.Flags().String("name", "", "storage name (required)")
	cmd.Flags().String("path", "", "storage path (default: auto)")
	cmd.Flags().Uint32("storage-class", 1, "storage class")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newNodeListStorageCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list-storage [node-name-or-id]",
		Short: "List file storages for a node (or all nodes)",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}

			var filterNodeID string
			if len(args) > 0 {
				r, err := newResolver(context.Background(), client)
				if err != nil {
					return err
				}
				filterNodeID, err = resolve(args[0], r.nodes, "node")
				if err != nil {
					return err
				}
			}

			// Build node name lookup.
			nodeNames := make(map[string]string)
			for _, n := range resp.Msg.NodeConfigs {
				nodeNames[glid.FromBytes(n.Id).String()] = n.Name
			}

			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.NodeStorageConfigs)
			}
			var rows [][]string
			for _, nsc := range resp.Msg.NodeStorageConfigs {
				if filterNodeID != "" && string(nsc.NodeId) != filterNodeID {
					continue
				}
				nscNodeStr := string(nsc.NodeId)
				nodeName := nodeNames[nscNodeStr]
				if nodeName == "" && len(nscNodeStr) > 8 {
					nodeName = nscNodeStr[:8]
				}
				for _, fs := range nsc.FileStorages {
					rows = append(rows, []string{
						nodeName, glid.FromBytes(fs.Id).String(), fs.Name,
						strconv.FormatUint(uint64(fs.StorageClass), 10),
						fs.Path,
					})
				}
			}
			p.table([]string{"NODE", "STORAGE ID", "NAME", "CLASS", "PATH"}, rows)
			return nil
		},
	}
}
