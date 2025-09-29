package main

import (
	"context"
	"log"
	"os"

	"github.com/rs/zerolog"
)

// Example of how to use the betfair-go library as a package
// Note: This example shows the intended usage when the library is imported as a package
func main() {
	// Create logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// This is an example of how you would use the library in your own project:
	//
	// import "github.com/felixmccuaig/betfair-go"
	//
	// cfg := betfair.NewConfig()
	// if err := cfg.LoadFromEnv(); err != nil {
	//     log.Fatal("Failed to load config:", err)
	// }
	//
	// recorder, err := betfair.NewMarketRecorder(cfg, logger)
	// if err != nil {
	//     log.Fatal("Failed to create recorder:", err)
	// }
	//
	// ctx := context.Background()
	// if err := recorder.Run(ctx); err != nil {
	//     log.Fatal("Recorder failed:", err)
	// }

	logger.Info().Msg("This is an example of how to use betfair-go as a library")
	logger.Info().Msg("See the enriched_recorder.go for a working example")
}