package main

import (
	"context"
	"log"

	betfair "github.com/felixmccuaig/betfair-go"
	"github.com/rs/zerolog"
)

func main() {
	// Example showing how to use just the streaming components
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create authenticator
	auth := betfair.NewAuthenticator("your-app-key", "username", "password")
	sessionToken, err := auth.Login()
	if err != nil {
		log.Fatal("Authentication failed:", err)
	}

	// Create stream client
	streamClient := betfair.NewStreamClient("your-app-key", sessionToken, 5000, logger, auth)

	// Dial connection
	stream, err := streamClient.Dial()
	if err != nil {
		log.Fatal("Failed to dial:", err)
	}
	defer stream.Close()

	// Authenticate
	if err := streamClient.Authenticate(stream); err != nil {
		log.Fatal("Authentication failed:", err)
	}

	// Request heartbeat
	if err := streamClient.RequestHeartbeat(stream); err != nil {
		log.Fatal("Heartbeat request failed:", err)
	}

	// Subscribe to markets
	filter := betfair.MarketFilter{
		EventTypeIDs: []string{"4339"}, // Greyhounds
		CountryCodes: []string{"AU"},   // Australia
	}

	if err := streamClient.Subscribe(stream, filter, "", ""); err != nil {
		log.Fatal("Subscription failed:", err)
	}

	// Process messages
	for {
		payload, err := stream.ReadMessage()
		if err != nil {
			log.Fatal("Read message failed:", err)
		}

		op := betfair.ExtractOp(payload)
		if op == "mcm" {
			marketID := betfair.ExtractMarketID(payload)
			if marketID != "" {
				logger.Info().Str("market_id", marketID).Msg("received market data")
			}
		}
	}
}