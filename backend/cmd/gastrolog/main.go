// Command gastrolog runs the log aggregation service.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	configfile "gastrolog/internal/config/file"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/receiver/chatterbox"
	"gastrolog/internal/source"
	sourcefile "gastrolog/internal/source/file"
)

func main() {
	configPath := flag.String("config", "config.json", "path to configuration file")
	sourcesPath := flag.String("sources", "sources.db", "path to sources registry file")
	pprofAddr := flag.String("pprof", "", "pprof HTTP server address (e.g. localhost:6060)")
	flag.Parse()

	if *pprofAddr != "" {
		go func() {
			log.Printf("pprof server listening on %s", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx, *configPath, *sourcesPath); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, configPath, sourcesPath string) error {
	// Load configuration.
	log.Printf("loading config from %s", configPath)
	cfgStore := configfile.NewStore(configPath)
	cfg, err := cfgStore.Load(ctx)
	if err != nil {
		return err
	}
	if cfg == nil {
		log.Printf("no config found, running with empty configuration")
	} else {
		log.Printf("loaded config: %d receivers, %d stores, %d routes",
			len(cfg.Receivers), len(cfg.Stores), len(cfg.Routes))
	}

	// Create source registry.
	sourceStore := sourcefile.NewStore(sourcesPath)
	sources, err := source.NewRegistry(source.Config{
		Store: sourceStore,
	})
	if err != nil {
		return err
	}
	defer sources.Close()

	// Create orchestrator.
	orch := orchestrator.New(orchestrator.Config{
		Sources: sources,
	})

	// Apply configuration with factories.
	if err := orch.ApplyConfig(cfg, buildFactories()); err != nil {
		return err
	}

	// Start the orchestrator.
	log.Printf("starting orchestrator")
	if err := orch.Start(ctx); err != nil {
		return err
	}
	log.Printf("orchestrator started, waiting for shutdown signal")

	// Wait for shutdown signal.
	<-ctx.Done()

	// Stop the orchestrator.
	log.Printf("shutting down")
	if err := orch.Stop(); err != nil {
		return err
	}
	log.Printf("shutdown complete")
	return nil
}

// buildFactories creates the factory maps for all supported component types.
func buildFactories() orchestrator.Factories {
	return orchestrator.Factories{
		Receivers: map[string]orchestrator.ReceiverFactory{
			"chatterbox": chatterbox.NewReceiver,
		},
		ChunkManagers: map[string]chunk.ManagerFactory{
			"file":   chunkfile.NewFactory(),
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"file":   indexfile.NewFactory(),
			"memory": indexmem.NewFactory(),
		},
	}
}
