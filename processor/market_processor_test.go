package processor

import (
	"os"
	"testing"
	"time"
)

func TestNewMarketDataProcessor(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 0)

	if processor == nil {
		t.Fatal("NewMarketDataProcessor returned nil")
	}

	if processor.OutputDir != "processed_market_data" {
		t.Errorf("Expected default output dir 'processed_market_data', got '%s'", processor.OutputDir)
	}

	if processor.Workers <= 0 {
		t.Error("Expected positive number of workers")
	}

	if processor.MarketStates == nil {
		t.Error("MarketStates map should be initialized")
	}
}

func TestExtractVenueFromEventName(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	tests := []struct {
		name      string
		eventName string
		expected  string
	}{
		{
			name:      "Standard venue format",
			eventName: "Sandown Park (VIC) R11 515m Heat",
			expected:  "Sandown Park",
		},
		{
			name:      "Queensland venue",
			eventName: "Ipswich (QLD) R7 Mixed 4/5",
			expected:  "Ipswich",
		},
		{
			name:      "No venue code",
			eventName: "Healesville R1",
			expected:  "Healesville R1",
		},
		{
			name:      "Empty event name",
			eventName: "",
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processor.extractVenueFromEventName(tt.eventName)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestExtractGreyhoundName(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	tests := []struct {
		name       string
		runnerName string
		expected   string
	}{
		{
			name:       "Standard greyhound name",
			runnerName: "1. Fantastic Nadia",
			expected:   "Fantastic Nadia",
		},
		{
			name:       "Double digit number",
			runnerName: "12. Blazing Bella",
			expected:   "Blazing Bella",
		},
		{
			name:       "No number prefix",
			runnerName: "Speed Demon",
			expected:   "Speed Demon",
		},
		{
			name:       "Empty runner name",
			runnerName: "",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processor.extractGreyhoundName(tt.runnerName)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestIsGreyhoundWinMarket(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	tests := []struct {
		name      string
		marketDef map[string]interface{}
		expected  bool
	}{
		{
			name: "Valid greyhound WIN market",
			marketDef: map[string]interface{}{
				"eventTypeId": "4339",
				"marketType":  "WIN",
				"bettingType": "ODDS",
			},
			expected: true,
		},
		{
			name: "Wrong event type (horse racing)",
			marketDef: map[string]interface{}{
				"eventTypeId": "7",
				"marketType":  "WIN",
				"bettingType": "ODDS",
			},
			expected: false,
		},
		{
			name: "Wrong market type (PLACE)",
			marketDef: map[string]interface{}{
				"eventTypeId": "4339",
				"marketType":  "PLACE",
				"bettingType": "ODDS",
			},
			expected: false,
		},
		{
			name: "Wrong betting type",
			marketDef: map[string]interface{}{
				"eventTypeId": "4339",
				"marketType":  "WIN",
				"bettingType": "ASIAN_HANDICAP",
			},
			expected: false,
		},
		{
			name: "Missing fields",
			marketDef: map[string]interface{}{
				"eventTypeId": "4339",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processor.isGreyhoundWinMarket(tt.marketDef)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestGetPrice30sBeforeStart(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	marketTime := time.Date(2025, 9, 29, 12, 0, 0, 0, time.UTC)

	// 30 seconds before market time
	targetTime := marketTime.Add(-30 * time.Second).UnixMilli()

	tests := []struct {
		name        string
		updates     []RunnerUpdate
		expectedPrice float64
		expectedHas bool
	}{
		{
			name: "Exact match 30s before",
			updates: []RunnerUpdate{
				{
					Timestamp: targetTime,
					LTP:       2.5,
					HasLTP:    true,
				},
			},
			expectedPrice: 2.5,
			expectedHas:   true,
		},
		{
			name: "Use BATB when no LTP",
			updates: []RunnerUpdate{
				{
					Timestamp: targetTime,
					BATB:      [][]float64{{3.0, 100.0}},
				},
			},
			expectedPrice: 3.0,
			expectedHas:   true,
		},
		{
			name: "No price data",
			updates: []RunnerUpdate{
				{
					Timestamp: targetTime,
				},
			},
			expectedPrice: 0,
			expectedHas:   false,
		},
		{
			name:        "No updates",
			updates:     []RunnerUpdate{},
			expectedPrice: 0,
			expectedHas:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			price, has := processor.getPrice30sBeforeStart(tt.updates, marketTime)
			if price != tt.expectedPrice {
				t.Errorf("Expected price %f, got %f", tt.expectedPrice, price)
			}
			if has != tt.expectedHas {
				t.Errorf("Expected has %v, got %v", tt.expectedHas, has)
			}
		})
	}
}

func TestProcessMCMMessage(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	// Test data representing a greyhound WIN market
	mcmData := map[string]interface{}{
		"op": "mcm",
		"pt": float64(1633024800000),
		"mc": []interface{}{
			map[string]interface{}{
				"id": "1.248346199",
				"marketDefinition": map[string]interface{}{
					"eventTypeId": "4339",
					"marketType":  "WIN",
					"bettingType": "ODDS",
					"eventName":   "Sandown Park (VIC) R11 515m Heat",
					"marketTime":  "2025-09-29T12:00:00Z",
					"runners": []interface{}{
						map[string]interface{}{
							"id":     float64(12345),
							"name":   "1. Test Greyhound",
							"bsp":    float64(2.5),
							"status": "ACTIVE",
						},
					},
				},
			},
		},
	}

	processor.processMCMMessage(mcmData)

	if len(processor.MarketStates) != 1 {
		t.Errorf("Expected 1 market state, got %d", len(processor.MarketStates))
	}

	market, exists := processor.MarketStates["1.248346199"]
	if !exists {
		t.Fatal("Market state not created")
	}

	if market.Venue != "Sandown Park" {
		t.Errorf("Expected venue 'Sandown Park', got '%s'", market.Venue)
	}

	if len(market.Runners) != 1 {
		t.Errorf("Expected 1 runner, got %d", len(market.Runners))
	}

	runner, exists := market.Runners[12345]
	if !exists {
		t.Fatal("Runner not created")
	}

	if runner.Name != "Test Greyhound" {
		t.Errorf("Expected runner name 'Test Greyhound', got '%s'", runner.Name)
	}

	if runner.BSP != 2.5 {
		t.Errorf("Expected BSP 2.5, got %f", runner.BSP)
	}
}

func TestProcessFileWithGreyhoundData(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	// Create test data that represents real greyhound market data
	testData := []string{
		`{"op":"mcm","pt":1633024800000,"mc":[{"id":"1.test","marketDefinition":{"eventTypeId":"4339","marketType":"WIN","bettingType":"ODDS","eventName":"Test Track R1","marketTime":"2025-09-29T12:00:00Z","runners":[{"id":123,"name":"1. Test Dog","bsp":2.5,"status":"ACTIVE"}]}}]}`,
		`{"op":"mcm","pt":1633024801000,"mc":[{"id":"1.test","rc":[{"id":123,"ltp":2.4,"tv":100.5}]}]}`,
		`{"op":"mcm","pt":1633024802000,"mc":[{"id":"1.test","marketDefinition":{"runners":[{"id":123,"status":"WINNER"}]}}]}`,
	}

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "test_greyhound_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Write test data
	for _, line := range testData {
		if _, err := tmpFile.WriteString(line + "\n"); err != nil {
			t.Fatalf("Failed to write test data: %v", err)
		}
	}
	tmpFile.Close()

	// Process the file
	err = processor.ProcessFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("ProcessFile failed: %v", err)
	}

	// Verify results
	if len(processor.MarketStates) != 1 {
		t.Errorf("Expected 1 market state, got %d", len(processor.MarketStates))
	}

	market, exists := processor.MarketStates["1.test"]
	if !exists {
		t.Fatal("Market not found")
	}

	runner, exists := market.Runners[123]
	if !exists {
		t.Fatal("Runner not found")
	}

	if runner.Name != "Test Dog" {
		t.Errorf("Expected runner name 'Test Dog', got '%s'", runner.Name)
	}

	if runner.LatestLTP != 2.4 {
		t.Errorf("Expected latest LTP 2.4, got %f", runner.LatestLTP)
	}

	if runner.MaxTV != 100.5 {
		t.Errorf("Expected max TV 100.5, got %f", runner.MaxTV)
	}

	if runner.Status != "WINNER" {
		t.Errorf("Expected status 'WINNER', got '%s'", runner.Status)
	}
}

func TestFinalizeMarket(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	// Set up a test market
	marketTime := time.Date(2025, 9, 29, 12, 0, 0, 0, time.UTC)
	processor.MarketStates["1.test"] = &MarketState{
		MarketTime: marketTime,
		Venue:      "Test Track",
		Runners: map[int64]*RunnerState{
			123: {
				Name:      "Test Winner",
				BSP:       2.5,
				LatestLTP: 2.4,
				MaxTV:     1000.0,
				Status:    "WINNER",
				Updates:   []RunnerUpdate{},
			},
			456: {
				Name:      "Test Loser",
				BSP:       5.0,
				LatestLTP: 4.8,
				MaxTV:     500.0,
				Status:    "LOSER",
				Updates:   []RunnerUpdate{},
			},
		},
	}

	// Finalize the market
	summaryRows := processor.finalizeMarket("1.test")

	if len(summaryRows) != 2 {
		t.Errorf("Expected 2 summary rows, got %d", len(summaryRows))
	}

	// Check that market was removed from processor
	if _, exists := processor.MarketStates["1.test"]; exists {
		t.Error("Market should have been removed after finalization")
	}

	// Verify winner row
	var winnerRow *SummaryRow
	for i := range summaryRows {
		if summaryRows[i].Win {
			winnerRow = &summaryRows[i]
			break
		}
	}

	if winnerRow == nil {
		t.Fatal("No winner row found")
	}

	if winnerRow.GreyhoundName != "Test Winner" {
		t.Errorf("Expected winner name 'Test Winner', got '%s'", winnerRow.GreyhoundName)
	}

	if winnerRow.MarketID != "1.test" {
		t.Errorf("Expected market ID '1.test', got '%s'", winnerRow.MarketID)
	}

	if winnerRow.Year != 2025 || winnerRow.Month != 9 || winnerRow.Day != 29 {
		t.Errorf("Expected date 2025-9-29, got %d-%d-%d", winnerRow.Year, winnerRow.Month, winnerRow.Day)
	}
}

func TestConvertToFloat64Array(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected [][]float64
	}{
		{
			name: "Valid price data",
			input: []interface{}{
				[]interface{}{float64(2.5), float64(100.0)},
				[]interface{}{float64(2.4), float64(50.0)},
			},
			expected: [][]float64{
				{2.5, 100.0},
				{2.4, 50.0},
			},
		},
		{
			name:     "Empty input",
			input:    []interface{}{},
			expected: [][]float64{},
		},
		{
			name: "Mixed invalid data",
			input: []interface{}{
				[]interface{}{float64(2.5), float64(100.0)},
				"invalid",
				[]interface{}{float64(2.4), "invalid"},
			},
			expected: [][]float64{
				{2.5, 100.0},
				{2.4},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertToFloat64Array(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d arrays, got %d", len(tt.expected), len(result))
				return
			}

			for i, expectedArray := range tt.expected {
				if len(result[i]) != len(expectedArray) {
					t.Errorf("Array %d: expected length %d, got %d", i, len(expectedArray), len(result[i]))
					continue
				}

				for j, expectedVal := range expectedArray {
					if result[i][j] != expectedVal {
						t.Errorf("Array %d, index %d: expected %f, got %f", i, j, expectedVal, result[i][j])
					}
				}
			}
		})
	}
}

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		name     string
		value    float64
		hasValue bool
		expected string
	}{
		{
			name:     "Valid value",
			value:    2.5,
			hasValue: true,
			expected: "2.5",
		},
		{
			name:     "Zero value with hasValue true",
			value:    0.0,
			hasValue: true,
			expected: "",
		},
		{
			name:     "Value with hasValue false",
			value:    2.5,
			hasValue: false,
			expected: "",
		},
		{
			name:     "Large value",
			value:    1234.56789,
			hasValue: true,
			expected: "1234.56789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatFloat(tt.value, tt.hasValue)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestProcessBzipCompressedFile(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	// For this test, we'll just verify the file extension detection works
	// since we can't easily create a proper bzip2 file in the test
	if !processor.isSupportedFile("test.bz2") {
		t.Error("Should support .bz2 files")
	}

	if !processor.isSupportedFile("test.json") {
		t.Error("Should support .json files")
	}

	if processor.isSupportedFile(".hidden") {
		t.Error("Should not support hidden files")
	}

	if processor.isSupportedFile("test.txt") {
		t.Error("Should not support .txt files")
	}
}

// Integration test: Process clean single-market file
func TestIntegrationCleanSingleMarketFile(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	// Process the clean test file
	err := processor.ProcessFile("testdata/clean_single_market.json")
	if err != nil {
		t.Fatalf("Failed to process clean file: %v", err)
	}

	// Verify market was processed correctly
	if len(processor.MarketStates) != 1 {
		t.Errorf("Expected 1 market state, got %d", len(processor.MarketStates))
	}

	market, exists := processor.MarketStates["1.248394055"]
	if !exists {
		t.Fatal("Market 1.248394055 not found")
	}

	// Verify venue extraction
	if market.Venue != "Warragul" {
		t.Errorf("Expected venue 'Warragul', got '%s'", market.Venue)
	}

	// Verify 3 runners
	if len(market.Runners) != 3 {
		t.Errorf("Expected 3 runners, got %d", len(market.Runners))
	}

	// Verify winner status
	winner, exists := market.Runners[47730803]
	if !exists {
		t.Fatal("Winner runner 47730803 not found")
	}

	if winner.Status != "WINNER" {
		t.Errorf("Expected status 'WINNER', got '%s'", winner.Status)
	}

	if winner.Name != "Fast Runner" {
		t.Errorf("Expected name 'Fast Runner', got '%s'", winner.Name)
	}

	if winner.BSP != 2.8 {
		t.Errorf("Expected BSP 2.8, got %f", winner.BSP)
	}

	// Verify price updates
	if winner.LatestLTP != 3.0 {
		t.Errorf("Expected latest LTP 3.0, got %f", winner.LatestLTP)
	}

	if winner.MaxTV != 400.0 {
		t.Errorf("Expected max TV 400.0, got %f", winner.MaxTV)
	}
}

// Integration test: Detect contamination in multi-market file
func TestIntegrationDetectContamination(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	// Process contaminated file (simulating file named after first market)
	err := processor.ProcessFile("testdata/contaminated_multi_market.json")
	if err != nil {
		t.Fatalf("Failed to process contaminated file: %v", err)
	}

	// Should have detected 3 markets in the file
	if len(processor.MarketStates) != 3 {
		t.Errorf("Expected 3 market states, got %d", len(processor.MarketStates))
	}

	// Verify all three markets were processed
	markets := []string{"1.248394055", "1.248394060", "1.248394065"}
	for _, marketID := range markets {
		if _, exists := processor.MarketStates[marketID]; !exists {
			t.Errorf("Market %s not found in processor state", marketID)
		}
	}

	// Verify individual market data
	market1, _ := processor.MarketStates["1.248394055"]
	if market1.Venue != "Warragul" {
		t.Errorf("Market 1: Expected venue 'Warragul', got '%s'", market1.Venue)
	}

	market2, _ := processor.MarketStates["1.248394060"]
	if market2.Venue != "Sandown Park" {
		t.Errorf("Market 2: Expected venue 'Sandown Park', got '%s'", market2.Venue)
	}

	market3, _ := processor.MarketStates["1.248394065"]
	if market3.Venue != "Angle Park" {
		t.Errorf("Market 3: Expected venue 'Angle Park', got '%s'", market3.Venue)
	}
}

// Integration test: Venue extraction when eventName is null
func TestIntegrationVenueExtractionNoEventName(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	// Process file with null eventName but valid venue field
	err := processor.ProcessFile("testdata/venue_only_no_eventname.json")
	if err != nil {
		t.Fatalf("Failed to process file: %v", err)
	}

	// Verify market was processed
	if len(processor.MarketStates) != 1 {
		t.Errorf("Expected 1 market state, got %d", len(processor.MarketStates))
	}

	market, exists := processor.MarketStates["1.248394060"]
	if !exists {
		t.Fatal("Market 1.248394060 not found")
	}

	// Critical test: Venue should be extracted from venue field, not eventName
	if market.Venue != "Warragul" {
		t.Errorf("Expected venue 'Warragul' (from venue field), got '%s'", market.Venue)
	}

	// EventName should be empty since it was null
	if market.EventName != "" {
		t.Errorf("Expected empty event name, got '%s'", market.EventName)
	}

	// Verify runner data
	runner, exists := market.Runners[47730801]
	if !exists {
		t.Fatal("Runner 47730801 not found")
	}

	if runner.Status != "WINNER" {
		t.Errorf("Expected status 'WINNER', got '%s'", runner.Status)
	}
}

// Integration test: Contamination detection with filename validation
func TestIntegrationContaminationDetectionWithFilename(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	// Create a temporary file with contaminated data but named after first market
	tmpFile, err := os.CreateTemp("", "1.248394055*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write contaminated data (contains markets 1.248394055, 1.248394060, 1.248394065)
	contaminatedData := `{"op":"mcm","pt":1727606400000,"clk":"12345","mc":[{"id":"1.248394055","marketDefinition":{"eventTypeId":"4339","marketType":"WIN","bettingType":"ODDS","venue":"Warragul","marketTime":"2025-09-29T12:00:00.000Z","runners":[{"id":47730801,"name":"1. Dog A","bsp":3.5,"status":"ACTIVE"}]}},{"id":"1.248394060","marketDefinition":{"eventTypeId":"4339","marketType":"WIN","bettingType":"ODDS","venue":"Sandown","marketTime":"2025-09-29T12:15:00.000Z","runners":[{"id":47730901,"name":"1. Dog B","bsp":2.5,"status":"ACTIVE"}]}}]}
{"op":"mcm","pt":1727606401000,"clk":"12346","mc":[{"id":"1.248394055","rc":[{"id":47730801,"ltp":3.4}]},{"id":"1.248394060","rc":[{"id":47730901,"ltp":2.6}]}]}
`
	if _, err := tmpFile.WriteString(contaminatedData); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	// Process the file - this should detect contamination
	err = processor.ProcessFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to process file: %v", err)
	}

	// Should detect 2 markets
	if len(processor.MarketStates) != 2 {
		t.Errorf("Expected 2 markets, got %d", len(processor.MarketStates))
	}

	// Both markets should be present
	if _, exists := processor.MarketStates["1.248394055"]; !exists {
		t.Error("Market 1.248394055 not found")
	}

	if _, exists := processor.MarketStates["1.248394060"]; !exists {
		t.Error("Market 1.248394060 not found (contamination)")
	}
}

// Test venue extraction priority: venue field takes precedence over eventName
func TestVenueExtractionPriority(t *testing.T) {
	processor := NewMarketDataProcessor("", 0, 1)

	tests := []struct {
		name             string
		venue            interface{} // can be string or nil
		eventName        interface{} // can be string or nil
		expectedVenue    string
		expectedEventName string
	}{
		{
			name:             "Both venue and eventName present",
			venue:            "Warragul",
			eventName:        "Sandown Park (VIC) R1",
			expectedVenue:    "Warragul", // venue field takes priority
			expectedEventName: "Sandown Park (VIC) R1",
		},
		{
			name:             "Only venue field present",
			venue:            "Warragul",
			eventName:        nil,
			expectedVenue:    "Warragul",
			expectedEventName: "",
		},
		{
			name:             "Only eventName present",
			venue:            nil,
			eventName:        "Sandown Park (VIC) R1",
			expectedVenue:    "Sandown Park", // extracted from eventName
			expectedEventName: "Sandown Park (VIC) R1",
		},
		{
			name:             "Neither present",
			venue:            nil,
			eventName:        nil,
			expectedVenue:    "",
			expectedEventName: "",
		},
		{
			name:             "Empty strings",
			venue:            "",
			eventName:        "",
			expectedVenue:    "",
			expectedEventName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test MCM message
			marketDef := map[string]interface{}{
				"eventTypeId": "4339",
				"marketType":  "WIN",
				"bettingType": "ODDS",
				"marketTime":  "2025-09-29T12:00:00.000Z",
				"runners": []interface{}{
					map[string]interface{}{
						"id":     float64(12345),
						"name":   "1. Test Dog",
						"bsp":    float64(2.5),
						"status": "ACTIVE",
					},
				},
			}

			// Add venue and eventName based on test case
			if tt.venue != nil {
				marketDef["venue"] = tt.venue
			}
			if tt.eventName != nil {
				marketDef["eventName"] = tt.eventName
			}

			mcmData := map[string]interface{}{
				"op": "mcm",
				"pt": float64(1633024800000),
				"mc": []interface{}{
					map[string]interface{}{
						"id":               "1.test",
						"marketDefinition": marketDef,
					},
				},
			}

			// Reset processor state
			processor.MarketStates = make(map[string]*MarketState)

			// Process the message
			processor.processMCMMessage(mcmData)

			// Verify venue extraction
			market, exists := processor.MarketStates["1.test"]
			if !exists {
				t.Fatal("Market not created")
			}

			if market.Venue != tt.expectedVenue {
				t.Errorf("Expected venue '%s', got '%s'", tt.expectedVenue, market.Venue)
			}

			if market.EventName != tt.expectedEventName {
				t.Errorf("Expected eventName '%s', got '%s'", tt.expectedEventName, market.EventName)
			}
		})
	}
}
