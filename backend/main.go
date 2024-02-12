package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/kluzzebass/gastrolog/internal/ingester"
	"github.com/kluzzebass/gastrolog/internal/ingester/syslog"
)

func main() {

	// set up slog

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		// AddSource: true,
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	gastrolog()
}

func gastrolog() {

	addr := ":5140"
	opt := &syslog.SyslogTCPIngesterOptions{
		Addr: &addr,
	}

	var i ingester.Ingester
	var err error

	i, err = syslog.NewSyslogTCPIngester(context.Background(), opt)
	if err != nil {
		log.Fatal(err)
	}

	// sleep for 1 minute
	time.Sleep(10 * time.Second)

	// cancel the ingester
	i.Cancel()

}
