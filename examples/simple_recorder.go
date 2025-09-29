package main

import (
	"context"
	"log"
	"os"

	betfair "github.com/felixmccuaig/betfair-go"
	"github.com/rs/zerolog"
)

// Example of how to use the betfair-go library as a package
func main() {
	// Create logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create and load configuration
	cfg := betfair.NewConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		log.Fatal("Failed to load config:", err)
	}

	// Create market recorder
	recorder, err := betfair.NewMarketRecorder(cfg, logger)
	if err != nil {
		log.Fatal("Failed to create recorder:", err)
	}

	// Start recording
	ctx := context.Background()
	if err := recorder.Run(ctx); err != nil {
		log.Fatal("Recorder failed:", err)
	}
}