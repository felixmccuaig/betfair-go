package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	betfair "github.com/felixmccuaig/betfair-go"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Configure logging early
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(os.Stderr)

	if err := godotenv.Load(); err != nil {
		log.Warn().Err(err).Msg("failed to load .env file (continuing without it)")
	}

	// Create and load configuration
	cfg := betfair.NewConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		log.Fatal().Err(err).Msg("failed to load configuration")
	}

	logger := log.With().Str("component", "enriched-market-recorder").Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Create market recorder with enrichment capabilities
	recorder, err := betfair.NewMarketRecorder(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create market recorder")
	}

	logger.Info().
		Strs("market_ids", cfg.MarketIDs).
		Str("event_type_id", cfg.EventTypeID).
		Str("country_code", cfg.CountryCode).
		Str("market_type", cfg.MarketType).
		Msg("starting enriched market recorder")

	logger.Info().Msg("market data will be enriched with runner names, event details, and venue information")

	if err := recorder.Run(ctx); err != nil {
		logger.Fatal().Err(err).Msg("recorder terminated")
	}
}

/*
Example enriched output (what you'll now see in your market files):

Before enrichment:
{
  "clk":"AMGr2gQAoJT2BADGyJQF",
  "mc":[{
    "id":"1.248304085",
    "img":true,
    "marketDefinition":{
      "betDelay":0,
      "bettingType":"ODDS",
      "bspMarket":true,
      "bspReconciled":false,
      "complete":true,
      "countryCode":"AU",
      "crossMatching":true,
      "discountAllowed":true,
      "eventId":"34780636",
      "eventTypeId":"4339",
      "inPlay":false,
      "marketBaseRate":8,
      "marketTime":"2025-09-28T01:17:00.000Z",
      "marketType":"WIN",
      "numberOfActiveRunners":7,
      "numberOfWinners":1,
      "openDate":"2025-09-28T01:17:00.000Z",
      "persistenceEnabled":false,
      "priceLadderDefinition":{"type":"CLASSIC"},
      "regulators":["MR_INT"],
      "runners":[
        {"id":89061103,"sortPriority":1,"status":"ACTIVE"},
        {"id":89061104,"sortPriority":2,"status":"ACTIVE"}
      ],
      "runnersVoidable":false,
      "status":"OPEN",
      "suspendTime":"2025-09-28T01:17:00.000Z",
      "timezone":"Australia/Sydney",
      "turnInPlayEnabled":false,
      "venue":"Healesville",
      "version":6886451024
    }
  }],
  "op":"mcm",
  "pt":1759007335101
}

After enrichment:
{
  "clk":"AMGr2gQAoJT2BADGyJQF",
  "mc":[{
    "id":"1.248304085",
    "img":true,
    "marketDefinition":{
      "betDelay":0,
      "bettingType":"ODDS",
      "bspMarket":true,
      "bspReconciled":false,
      "complete":true,
      "countryCode":"AU",
      "crossMatching":true,
      "discountAllowed":true,
      "eventId":"34780636",
      "eventName":"Healesville R1",                    // <- Added from market catalogue
      "eventTypeId":"4339",
      "eventTypeName":"Greyhound Racing",              // <- Added from market catalogue
      "inPlay":false,
      "marketBaseRate":8,
      "marketName":"Win",                              // <- Added from market catalogue
      "marketTime":"2025-09-28T01:17:00.000Z",
      "marketType":"WIN",
      "numberOfActiveRunners":7,
      "numberOfWinners":1,
      "openDate":"2025-09-28T01:17:00.000Z",
      "persistenceEnabled":false,
      "priceLadderDefinition":{"type":"CLASSIC"},
      "regulators":["MR_INT"],
      "runners":[
        {
          "id":89061103,
          "runnerName":"Fantastic Nadia",             // <- Added from market catalogue
          "sortPriority":1,
          "status":"ACTIVE"
        },
        {
          "id":89061104,
          "runnerName":"Blazing Bella",               // <- Added from market catalogue
          "sortPriority":2,
          "status":"ACTIVE"
        }
      ],
      "runnersVoidable":false,
      "status":"OPEN",
      "suspendTime":"2025-09-28T01:17:00.000Z",
      "timezone":"Australia/Sydney",
      "turnInPlayEnabled":false,
      "venue":"Healesville",
      "version":6886451024
    }
  }],
  "op":"mcm",
  "pt":1759007335101
}
*/