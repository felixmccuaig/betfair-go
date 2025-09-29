package main

import (
	"os"

	"github.com/rs/zerolog"
)

// Example of how to use the streaming components as a package
func main() {
	// Example showing how to use just the streaming components
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// This is an example of how you would use the streaming components in your own project:
	//
	// import "github.com/felixmccuaig/betfair-go"
	//
	// auth := betfair.NewAuthenticator("your-app-key", "username", "password")
	// sessionToken, err := auth.Login()
	// if err != nil {
	//     log.Fatal("Authentication failed:", err)
	// }
	//
	// streamClient := betfair.NewStreamClient("your-app-key", sessionToken, 5000, logger, auth)
	//
	// stream, err := streamClient.Dial()
	// if err != nil {
	//     log.Fatal("Failed to dial:", err)
	// }
	// defer stream.Close()
	//
	// ... continue with authentication, subscription, and message processing

	logger.Info().Msg("This is an example of how to use betfair-go streaming components as a library")
	logger.Info().Msg("See the enriched_recorder.go for a complete working example")
}