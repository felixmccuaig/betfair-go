package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Configure logging early so parseConfig can emit helpful errors.
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(os.Stderr)

	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn().Err(err).Msg("failed to load .env file")
	}

	cfg := NewConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		log.Fatal().Err(err).Msg("failed to load configuration")
	}

	logger := log.With().Str("component", "market-recorder").Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	recorder, err := NewMarketRecorder(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create market recorder")
	}

	logger.Info().Strs("market_ids", cfg.MarketIDs).Msg("starting market recorder")

	if err := recorder.Run(ctx); err != nil {
		logger.Fatal().Err(err).Msg("recorder terminated")
	}
}