// Send test syslog messages over RELP to a gastrolog RELP ingester.
//
// Usage:
//
//	go run test/relp_send.go [host:port] [count]
//
// Defaults to localhost:2514 and 5 messages.
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

func readResponse(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line[:len(line)-1], nil
}

func main() {
	addr := "localhost:2514"
	count := 5

	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	if len(os.Args) > 2 {
		n, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid count: %v\n", err)
			os.Exit(1)
		}
		count = n
	}

	fmt.Printf("Connecting to %s...\n", addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)

	// Open RELP session.
	offer := "relp_version=0\nrelp_software=gastrolog-test\ncommands=syslog"
	fmt.Fprintf(conn, "1 open %d %s\n", len(offer), offer)
	resp, err := readResponse(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  open -> %s\n", resp)

	// Send syslog messages.
	for i := range count {
		txnr := i + 2
		msg := fmt.Sprintf("<14>%s testhost myapp[%d]: RELP test message %d",
			time.Now().Format("Jan 02 15:04:05"), txnr, i)
		fmt.Fprintf(conn, "%d syslog %d %s\n", txnr, len(msg), msg)
		resp, err = readResponse(r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "msg %d: %v\n", i, err)
			os.Exit(1)
		}
		fmt.Printf("  msg %d -> %s\n", i, resp)
	}

	// Close session.
	closeTxnr := count + 2
	fmt.Fprintf(conn, "%d close 0 \n", closeTxnr)
	resp, err = readResponse(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  close -> %s\n", resp)

	fmt.Printf("Done, sent %d messages.\n", count)
}
