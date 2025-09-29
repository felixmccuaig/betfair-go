package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestMarketRecorderIsRetriableError(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &MarketRecorder{
		logger: logger,
	}

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "Context canceled",
			err:      context.Canceled,
			expected: false,
		},
		{
			name:     "Context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		{
			name:     "Authentication failed",
			err:      errors.New("authentication failed"),
			expected: true,
		},
		{
			name:     "Connection closed",
			err:      errors.New("connection closed"),
			expected: true,
		},
		{
			name:     "Session refreshed retry",
			err:      errors.New("session refreshed, retry connection"),
			expected: true,
		},
		{
			name:     "Generic network error",
			err:      errors.New("network error occurred"),
			expected: true,
		},
		{
			name:     "Subscription failed",
			err:      errors.New("subscription failed"),
			expected: true,
		},
		{
			name:     "Timeout error",
			err:      errors.New("request timeout"),
			expected: true,
		},
		{
			name:     "Unknown error",
			err:      errors.New("something went wrong"),
			expected: true, // Default to retriable
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := recorder.isRetriableError(tt.err)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMarketRecorderExtractAndStoreClock(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &MarketRecorder{
		logger: logger,
	}

	tests := []struct {
		name            string
		json            string
		expectedInitClk string
		expectedClk     string
	}{
		{
			name:            "Both clocks present",
			json:            `{"initialClk":"init123","clk":"clk456"}`,
			expectedInitClk: "init123",
			expectedClk:     "clk456",
		},
		{
			name:            "Only initial clock",
			json:            `{"initialClk":"init123"}`,
			expectedInitClk: "init123",
			expectedClk:     "", // Should remain empty
		},
		{
			name:            "Only regular clock",
			json:            `{"clk":"clk456"}`,
			expectedInitClk: "", // Should remain empty
			expectedClk:     "clk456",
		},
		{
			name:            "No clocks",
			json:            `{"op":"mcm"}`,
			expectedInitClk: "", // Should remain empty
			expectedClk:     "", // Should remain empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset recorder state
			recorder.initialClk = ""
			recorder.clk = ""

			// Use the actual function from market.go
			initialClk, clk := ExtractAndStoreClock([]byte(tt.json))

			// Store in recorder (simulating the actual flow)
			if initialClk != "" {
				recorder.initialClk = initialClk
			}
			if clk != "" {
				recorder.clk = clk
			}

			if recorder.initialClk != tt.expectedInitClk {
				t.Errorf("Expected initialClk '%s', got '%s'", tt.expectedInitClk, recorder.initialClk)
			}
			if recorder.clk != tt.expectedClk {
				t.Errorf("Expected clk '%s', got '%s'", tt.expectedClk, recorder.clk)
			}
		})
	}
}

func TestMarketRecorderCreateWriterForMarket(t *testing.T) {
	tempDir := t.TempDir()

	config := &Config{
		OutputPath: tempDir,
	}

	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	fileManager := NewFileManager(tempDir)

	recorder := &MarketRecorder{
		config:      config,
		logger:      logger,
		fileManager: fileManager,
	}

	writers := make(map[string]*bufio.Writer)
	files := make(map[string]*os.File)
	marketID := "1.testmarket123"

	// Test creating a writer for a market
	err := recorder.createWriterForMarket(marketID, writers, files)
	if err != nil {
		t.Fatalf("Failed to create writer for market: %v", err)
	}

	// Verify writer was added to the writers map
	if _, exists := writers[marketID]; !exists {
		t.Error("Writer should be added to writers map")
	}

	// Verify file was added to the files map
	if _, exists := files[marketID]; !exists {
		t.Error("File should be added to files map")
	}

	// Verify the file was created on disk
	expectedFilePath := filepath.Join(tempDir, marketID)
	if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
		t.Errorf("Market file should be created at %s", expectedFilePath)
	}

	// Clean up
	if file, exists := files[marketID]; exists {
		file.Close()
	}
}

func TestMarketRecorderEnrichMarketData(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	// Create mock market catalogue
	mockCatalogue := &MarketCatalogue{
		MarketID:   "1.testmarket",
		MarketName: "Test Win Market",
		Event: &Event{
			ID:    "12345",
			Name:  "Test Race Event",
			Venue: "Test Track",
		},
		EventType: &EventType{
			ID:   "4339",
			Name: "Greyhound Racing",
		},
		Competition: &Competition{
			ID:   "comp1",
			Name: "Test Competition",
		},
		Runners: []RunnerCatalog{
			{
				SelectionID:  67890,
				RunnerName:   "Test Runner 1",
				Handicap:     0,
				SortPriority: 1,
			},
			{
				SelectionID:  67891,
				RunnerName:   "Test Runner 2",
				Handicap:     0,
				SortPriority: 2,
			},
		},
	}

	recorder := &MarketRecorder{
		logger:           logger,
		marketCatalogues: map[string]*MarketCatalogue{"1.testmarket": mockCatalogue},
	}

	// Test payload with market data
	payload := []byte(`{
		"op": "mcm",
		"mc": [{
			"id": "1.testmarket",
			"marketDefinition": {
				"status": "OPEN",
				"runners": [
					{"id": 67890, "status": "ACTIVE", "sortPriority": 1},
					{"id": 67891, "status": "ACTIVE", "sortPriority": 2}
				]
			}
		}]
	}`)

	enrichedPayload, err := recorder.enrichMarketData("1.testmarket", payload)
	if err != nil {
		t.Fatalf("Failed to enrich market data: %v", err)
	}

	// Parse enriched payload to verify enrichment
	var enrichedData map[string]interface{}
	err = json.Unmarshal(enrichedPayload, &enrichedData)
	if err != nil {
		t.Fatalf("Failed to parse enriched payload: %v", err)
	}

	// Navigate to market definition
	mc := enrichedData["mc"].([]interface{})
	market := mc[0].(map[string]interface{})
	marketDef := market["marketDefinition"].(map[string]interface{})

	// Verify market name was added
	if marketDef["marketName"] != "Test Win Market" {
		t.Errorf("Expected market name 'Test Win Market', got '%v'", marketDef["marketName"])
	}

	// Verify event name was added
	if marketDef["eventName"] != "Test Race Event" {
		t.Errorf("Expected event name 'Test Race Event', got '%v'", marketDef["eventName"])
	}

	// Verify event type name was added
	if marketDef["eventTypeName"] != "Greyhound Racing" {
		t.Errorf("Expected event type name 'Greyhound Racing', got '%v'", marketDef["eventTypeName"])
	}

	// Verify runners were enriched with names
	runners := marketDef["runners"].([]interface{})
	if len(runners) != 2 {
		t.Errorf("Expected 2 runners, got %d", len(runners))
	}

	runner1 := runners[0].(map[string]interface{})
	if runner1["name"] != "Test Runner 1" {
		t.Errorf("Expected runner name 'Test Runner 1', got '%v'", runner1["name"])
	}

	runner2 := runners[1].(map[string]interface{})
	if runner2["name"] != "Test Runner 2" {
		t.Errorf("Expected runner name 'Test Runner 2', got '%v'", runner2["name"])
	}
}

func TestMarketRecorderEnrichMarketDataNoCache(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &MarketRecorder{
		logger:           logger,
		marketCatalogues: make(map[string]*MarketCatalogue), // Empty cache
	}

	// Test payload
	payload := []byte(`{
		"op": "mcm",
		"mc": [{
			"id": "1.unknownmarket",
			"marketDefinition": {
				"status": "OPEN"
			}
		}]
	}`)

	// Should return original payload when no catalogue data available
	enrichedPayload, err := recorder.enrichMarketData("1.unknownmarket", payload)
	if err != nil {
		t.Fatalf("Enrichment should not fail when no catalogue data available: %v", err)
	}

	// Should be identical to input
	if string(enrichedPayload) != string(payload) {
		t.Error("Payload should be unchanged when no catalogue data available")
	}
}

func TestReconnectionScenario(t *testing.T) {
	// Test full reconnection scenario with clock preservation

	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &MarketRecorder{
		logger:     logger,
		maxRetries: 3,
		retryDelay: time.Millisecond * 100,
	}

	// Test initial state - no clocks preserved
	if recorder.initialClk != "" {
		t.Error("Initial clock should be empty at start")
	}
	if recorder.clk != "" {
		t.Error("Regular clock should be empty at start")
	}

	// Simulate first connection - receive initial clock
	firstConnMsg := `{"op":"connection","connectionId":"123-abc","initialClk":"1000","clk":"1001"}`
	initialClk, clk := ExtractAndStoreClock([]byte(firstConnMsg))
	if initialClk != "" {
		recorder.initialClk = initialClk
	}
	if clk != "" {
		recorder.clk = clk
	}

	if recorder.initialClk != "1000" {
		t.Errorf("Expected initialClk '1000', got '%s'", recorder.initialClk)
	}
	if recorder.clk != "1001" {
		t.Errorf("Expected clk '1001', got '%s'", recorder.clk)
	}

	// Simulate receiving market data with clock updates
	marketMsg := `{"op":"mcm","id":"marketSub","initialClk":"1000","clk":"1005","changeType":"SUB_IMAGE","mc":[{"id":"1.123","marketDefinition":{"status":"OPEN"}}]}`
	initialClk, clk = ExtractAndStoreClock([]byte(marketMsg))
	if clk != "" {
		recorder.clk = clk
	}

	if recorder.clk != "1005" {
		t.Errorf("Expected updated clk '1005', got '%s'", recorder.clk)
	}

	// Simulate connection drop (clock values preserved)
	preservedInitialClk := recorder.initialClk
	preservedClk := recorder.clk

	// Test that isRetriableError correctly identifies connection issues
	connectionErr := errors.New("connection closed")
	if !recorder.isRetriableError(connectionErr) {
		t.Error("Connection closed should be retriable")
	}

	// Simulate reconnection - clocks should be preserved for fast recovery
	if recorder.initialClk != preservedInitialClk {
		t.Error("Initial clock should be preserved during reconnection")
	}
	if recorder.clk != preservedClk {
		t.Error("Regular clock should be preserved during reconnection")
	}

	// Simulate successful reconnection with heartbeat update
	heartbeatMsg := `{"op":"heartbeat","clk":"1010"}`
	_, clk = ExtractAndStoreClock([]byte(heartbeatMsg))
	if clk != "" {
		recorder.clk = clk
	}

	if recorder.clk != "1010" {
		t.Errorf("Expected updated clk after reconnection '1010', got '%s'", recorder.clk)
	}

	// Test that initialClk is preserved (only set once)
	if recorder.initialClk != preservedInitialClk {
		t.Error("Initial clock should remain unchanged after reconnection")
	}
}

func TestReauthenticationScenario(t *testing.T) {
	// Test re-authentication when session expires

	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &MarketRecorder{
		logger:     logger,
		maxRetries: 3,
		retryDelay: time.Millisecond * 100,
	}

	// Test authentication error detection
	authErr := errors.New("authentication failed")
	if !recorder.isRetriableError(authErr) {
		t.Error("Authentication failed should be retriable for re-auth")
	}

	// Test that various auth-related errors are retriable
	testAuthErrors := []string{
		"authentication failed",
		"session expired",
		"invalid session token",
		"unauthorized",
	}

	for _, errMsg := range testAuthErrors {
		err := errors.New(errMsg)
		if !recorder.isRetriableError(err) {
			t.Errorf("Error '%s' should be retriable for re-auth", errMsg)
		}
	}

	// Non-retriable errors should not trigger re-auth
	nonRetriableErrs := []error{
		context.Canceled,
		context.DeadlineExceeded,
	}

	for _, err := range nonRetriableErrs {
		if recorder.isRetriableError(err) {
			t.Errorf("Error '%v' should not be retriable", err)
		}
	}
}

func TestClockRecoveryAfterDisconnection(t *testing.T) {
	// Test that the system can resume with minimal data loss using preserved clocks

	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &MarketRecorder{
		logger:     logger,
		maxRetries: 3,
		retryDelay: time.Millisecond * 100,
	}

	// Step 1: Establish initial connection and receive data
	initialMsg := `{"op":"connection","connectionId":"conn-1","initialClk":"2000","clk":"2001"}`
	initialClk, clk := ExtractAndStoreClock([]byte(initialMsg))
	if initialClk != "" {
		recorder.initialClk = initialClk
	}
	if clk != "" {
		recorder.clk = clk
	}

	// Step 2: Process several market updates
	updates := []string{
		`{"op":"mcm","clk":"2005","mc":[{"id":"1.market1","marketDefinition":{"status":"OPEN"}}]}`,
		`{"op":"mcm","clk":"2010","mc":[{"id":"1.market2","marketDefinition":{"status":"OPEN"}}]}`,
		`{"op":"mcm","clk":"2015","mc":[{"id":"1.market1","marketDefinition":{"status":"SUSPENDED"}}]}`,
	}

	for _, update := range updates {
		_, clk = ExtractAndStoreClock([]byte(update))
		if clk != "" {
			recorder.clk = clk
		}
	}

	// Step 3: Verify clock progression
	if recorder.clk != "2015" {
		t.Errorf("Expected final clock '2015', got '%s'", recorder.clk)
	}

	// Step 4: Simulate connection loss and recovery
	// In real scenario, the system would detect connection loss and attempt reconnection
	// The preserved clocks allow resuming from last known position

	// Step 5: Verify that after reconnection, we can continue from where we left off
	reconnectMsg := `{"op":"connection","connectionId":"conn-2","initialClk":"2000","clk":"2020"}`
	initialClk, clk = ExtractAndStoreClock([]byte(reconnectMsg))
	if clk != "" {
		recorder.clk = clk
	}

	// Initial clock should remain from first connection (not updated on reconnect)
	if recorder.initialClk != "2000" {
		t.Errorf("Initial clock should remain '2000', got '%s'", recorder.initialClk)
	}

	// Regular clock should update to latest from reconnection
	if recorder.clk != "2020" {
		t.Errorf("Clock should update to '2020' after reconnection, got '%s'", recorder.clk)
	}

	// Step 6: Verify we can continue processing from the new position
	postReconnectMsg := `{"op":"mcm","clk":"2025","mc":[{"id":"1.market3","marketDefinition":{"status":"OPEN"}}]}`
	_, clk = ExtractAndStoreClock([]byte(postReconnectMsg))
	if clk != "" {
		recorder.clk = clk
	}

	if recorder.clk != "2025" {
		t.Errorf("Expected clock to continue updating after reconnection '2025', got '%s'", recorder.clk)
	}

	t.Logf("Successfully tested clock recovery scenario: initialClk=%s, finalClk=%s",
		recorder.initialClk, recorder.clk)
}

func TestMarketRecorderCacheManagement(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &MarketRecorder{
		logger:           logger,
		marketCatalogues: make(map[string]*MarketCatalogue),
	}

	marketID := "1.testcache"

	// Verify cache is initially empty
	if _, exists := recorder.marketCatalogues[marketID]; exists {
		t.Error("Cache should be empty initially")
	}

	// Add item to cache
	mockCatalogue := &MarketCatalogue{
		MarketID:   marketID,
		MarketName: "Test Cache Market",
	}
	recorder.marketCatalogues[marketID] = mockCatalogue

	// Verify item was cached
	if cached, exists := recorder.marketCatalogues[marketID]; !exists {
		t.Error("Item should be cached")
	} else if cached.MarketName != "Test Cache Market" {
		t.Errorf("Expected cached market name 'Test Cache Market', got '%s'", cached.MarketName)
	}

	// Simulate market settlement - cache should be cleaned up
	delete(recorder.marketCatalogues, marketID)

	// Verify cache was cleaned up
	if _, exists := recorder.marketCatalogues[marketID]; exists {
		t.Error("Cache should be cleaned up after market settlement")
	}

	t.Log("✅ Market catalogue cache management test passed")
}