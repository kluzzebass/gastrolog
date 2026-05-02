// walinspect dumps the raft WAL into human-readable lines.
//
// Usage: go run ./cmd/walinspect data/node1/raft/wal [--cmd-only] [--filter-group=<id>]
//
// Reads every WAL segment in order, decodes group registrations, and for each
// log entry writes a line describing the raft index, term, log type, and (when
// the data is a chunk-FSM command) the command name + chunk id.
package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/tierfsm"

	hraft "github.com/hashicorp/raft"
)

const (
	entryLog         byte = 1
	entryStableSet   byte = 2
	entryStableU64   byte = 3
	entryDeleteRange byte = 4
	entryGroupReg    byte = 5

	headerSize = 13
)

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

//nolint:gocognit,gocyclo // CLI flag plumbing is intentionally straightforward.
func main() {
	var (
		cmdOnly     = flag.Bool("cmd-only", false, "only print FSM-command log entries")
		filterGroup = flag.String("filter-group", "", "only print entries from this group name (tier id, 'config', etc.)")
		filterCmd   = flag.String("filter-cmd", "", "only print entries with this FSM command (e.g. CmdDeleteChunk)")
		summary     = flag.Bool("summary", false, "print per-group + per-command counts only")
		termHist    = flag.Bool("term-hist", false, "print per-(group, term, cmd) counts to localize spikes")
	)
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: walinspect <wal-dir> [flags]")
		os.Exit(2)
	}
	dir := flag.Arg(0)

	segs, err := walSegments(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "list segments:", err)
		os.Exit(1)
	}

	groupName := map[uint32]string{}
	type stat struct {
		group string
		cmd   string
	}
	counts := map[stat]int{}

	type termStat struct {
		group string
		term  uint64
		cmd   string
	}
	termCounts := map[termStat]int{}

	for _, seg := range segs {
		if err := walkSegment(seg, func(gid uint32, typ byte, payload []byte) error {
			if typ == entryGroupReg {
				groupName[gid] = string(payload)
				return nil
			}

			if typ != entryLog {
				if *summary || *cmdOnly || *termHist || *filterCmd != "" {
					return nil
				}
				gn := groupNameOrID(groupName, gid)
				if *filterGroup != "" && gn != *filterGroup {
					return nil
				}
				fmt.Printf("seg=%-22s gid=%d (%s) typ=%s\n", filepath.Base(seg), gid, gn, typeName(typ))
				return nil
			}

			var lg hraft.Log
			if !tryDecodeLog(payload, &lg) {
				return nil // skip malformed entries, continue scan
			}
			cmdStr, chunkID := decodeFSMCmd(lg.Data, lg.Type)
			gn := groupNameOrID(groupName, gid)
			if *filterGroup != "" && gn != *filterGroup {
				return nil
			}
			if *filterCmd != "" && cmdStr != *filterCmd {
				return nil
			}
			if *summary {
				counts[stat{gn, cmdStr}]++
				return nil
			}
			if *termHist {
				termCounts[termStat{gn, lg.Term, cmdStr}]++
				return nil
			}
			if *cmdOnly && cmdStr == "" {
				return nil
			}
			fmt.Printf("seg=%-22s gid=%d (%s) idx=%d term=%d type=%s cmd=%s chunk=%s\n",
				filepath.Base(seg), gid, gn, lg.Index, lg.Term, raftLogType(lg.Type), cmdStr, chunkID)
			return nil
		}); err != nil {
			fmt.Fprintln(os.Stderr, "walk", seg, ":", err)
			os.Exit(1)
		}
	}

	if *summary {
		type row struct {
			stat
			n int
		}
		var rows []row
		for k, v := range counts {
			rows = append(rows, row{k, v})
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].group != rows[j].group {
				return rows[i].group < rows[j].group
			}
			return rows[i].cmd < rows[j].cmd
		})
		fmt.Println("group                       cmd                       count")
		for _, r := range rows {
			fmt.Printf("%-26s  %-24s  %d\n", r.group, r.cmd, r.n)
		}
	}

	if *termHist {
		type row struct {
			termStat
			n int
		}
		var rows []row
		for k, v := range termCounts {
			rows = append(rows, row{k, v})
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].group != rows[j].group {
				return rows[i].group < rows[j].group
			}
			if rows[i].term != rows[j].term {
				return rows[i].term < rows[j].term
			}
			return rows[i].cmd < rows[j].cmd
		})
		fmt.Println("group                       term  cmd                       count")
		for _, r := range rows {
			fmt.Printf("%-26s  %4d  %-24s  %d\n", r.group, r.term, r.cmd, r.n)
		}
	}
}

func walSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".log") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out, nil
}

func walkSegment(path string, visit func(gid uint32, typ byte, payload []byte) error) error {
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	hdr := make([]byte, headerSize)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		gid := binary.LittleEndian.Uint32(hdr[0:4])
		typ := hdr[4]
		length := int(binary.LittleEndian.Uint32(hdr[5:9]))
		stored := binary.LittleEndian.Uint32(hdr[9:13])
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			return nil // truncated tail
		}
		if crc32.Checksum(payload, crc32Table) != stored {
			return fmt.Errorf("crc mismatch in %s", filepath.Base(path))
		}
		if err := visit(gid, typ, payload); err != nil {
			return err
		}
	}
}

// decodeLog matches internal/raftwal.decodelog.
func decodeLog(data []byte, lg *hraft.Log) error {
	if len(data) < 21 {
		return errors.New("short log entry")
	}
	lg.Index = binary.LittleEndian.Uint64(data[0:8])
	lg.Term = binary.LittleEndian.Uint64(data[8:16])
	lg.Type = hraft.LogType(data[16])
	dataLen := int(binary.LittleEndian.Uint32(data[17:21]))
	if len(data) < 21+dataLen+4 {
		return errors.New("truncated log data")
	}
	lg.Data = make([]byte, dataLen)
	copy(lg.Data, data[21:21+dataLen])
	off := 21 + dataLen
	extLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
	if extLen > 0 && off+4+extLen <= len(data) {
		lg.Extensions = make([]byte, extLen)
		copy(lg.Extensions, data[off+4:off+4+extLen])
	}
	return nil
}

func tryDecodeLog(data []byte, lg *hraft.Log) bool {
	return decodeLog(data, lg) == nil
}

func decodeFSMCmd(data []byte, logType hraft.LogType) (string, string) {
	if logType != hraft.LogCommand || len(data) < 1 {
		return "", ""
	}
	cmd := tierfsm.Command(data[0])
	cmdName := commandName(cmd)
	if len(data) < 1+glid.Size {
		return cmdName, ""
	}
	return cmdName, glid.FromBytes(data[1 : 1+glid.Size]).String()
}

func commandName(cmd tierfsm.Command) string {
	switch cmd {
	case tierfsm.CmdCreateChunk:
		return "CmdCreateChunk"
	case tierfsm.CmdSealChunk:
		return "CmdSealChunk"
	case tierfsm.CmdCompressChunk:
		return "CmdCompressChunk"
	case tierfsm.CmdUploadChunk:
		return "CmdUploadChunk"
	case tierfsm.CmdDeleteChunk:
		return "CmdDeleteChunk"
	case tierfsm.CmdRetentionPending:
		return "CmdRetentionPending"
	case tierfsm.CmdTransitionStreamed:
		return "CmdTransitionStreamed"
	case tierfsm.CmdTransitionReceived:
		return "CmdTransitionReceived"
	case tierfsm.CmdRequestDelete:
		return "CmdRequestDelete"
	case tierfsm.CmdAckDelete:
		return "CmdAckDelete"
	case tierfsm.CmdFinalizeDelete:
		return "CmdFinalizeDelete"
	case tierfsm.CmdPruneNode:
		return "CmdPruneNode"
	default:
		return fmt.Sprintf("CmdUnknown(%d)", cmd)
	}
}

func raftLogType(t hraft.LogType) string {
	switch t {
	case hraft.LogCommand:
		return "Command"
	case hraft.LogNoop:
		return "Noop"
	case hraft.LogAddPeerDeprecated:
		return "AddPeerDeprecated"
	case hraft.LogRemovePeerDeprecated:
		return "RemovePeerDeprecated"
	case hraft.LogBarrier:
		return "Barrier"
	case hraft.LogConfiguration:
		return "Configuration"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

func typeName(t byte) string {
	switch t {
	case entryLog:
		return "entryLog"
	case entryStableSet:
		return "entryStableSet"
	case entryStableU64:
		return "entryStableU64"
	case entryDeleteRange:
		return "entryDeleteRange"
	case entryGroupReg:
		return "entryGroupReg"
	default:
		return fmt.Sprintf("entryUnknown(%d)", t)
	}
}

func groupNameOrID(groupName map[uint32]string, gid uint32) string {
	if name, ok := groupName[gid]; ok {
		return name
	}
	return fmt.Sprintf("gid:%d", gid)
}
