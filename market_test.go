package betfair

import (
	"encoding/json"
	"testing"
)

func TestExtractOp(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string
	}{
		{
			name:     "MCM operation",
			json:     `{"op":"mcm","id":3}`,
			expected: "mcm",
		},
		{
			name:     "Status operation",
			json:     `{"op":"status","statusCode":"SUCCESS"}`,
			expected: "status",
		},
		{
			name:     "Invalid JSON",
			json:     `{invalid}`,
			expected: "",
		},
		{
			name:     "Missing op field",
			json:     `{"id":3}`,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractOp([]byte(tt.json))
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestExtractMarketID(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string
	}{
		{
			name:     "Valid MCM with market ID",
			json:     `{"op":"mcm","mc":[{"id":"1.248231892"}]}`,
			expected: "1.248231892",
		},
		{
			name:     "Multiple markets, returns first",
			json:     `{"op":"mcm","mc":[{"id":"1.111111"},{"id":"1.222222"}]}`,
			expected: "1.111111",
		},
		{
			name:     "Empty MC array",
			json:     `{"op":"mcm","mc":[]}`,
			expected: "",
		},
		{
			name:     "No MC field",
			json:     `{"op":"mcm"}`,
			expected: "",
		},
		{
			name:     "Invalid JSON",
			json:     `{invalid}`,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractMarketID([]byte(tt.json))
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestExtractMarketStatus(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string
	}{
		{
			name:     "Open market",
			json:     `{"op":"mcm","mc":[{"marketDefinition":{"status":"OPEN"}}]}`,
			expected: "OPEN",
		},
		{
			name:     "Closed market",
			json:     `{"op":"mcm","mc":[{"marketDefinition":{"status":"CLOSED"}}]}`,
			expected: "CLOSED",
		},
		{
			name:     "Suspended market",
			json:     `{"op":"mcm","mc":[{"marketDefinition":{"status":"SUSPENDED"}}]}`,
			expected: "SUSPENDED",
		},
		{
			name:     "No market definition",
			json:     `{"op":"mcm","mc":[{}]}`,
			expected: "",
		},
		{
			name:     "Empty MC array",
			json:     `{"op":"mcm","mc":[]}`,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractMarketStatus([]byte(tt.json))
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestIsMarketSettled(t *testing.T) {
	tests := []struct {
		status   string
		expected bool
	}{
		{"OPEN", false},
		{"SUSPENDED", false},
		{"CLOSED", true},
		{"closed", false}, // Case sensitive
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			result := IsMarketSettled(tt.status)
			if result != tt.expected {
				t.Errorf("Expected %v for status '%s', got %v", tt.expected, tt.status, result)
			}
		})
	}
}

func TestExtractEventInfo(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		wantErr  bool
		expected *EventInfo
	}{
		{
			name: "Valid event info",
			json: `{"op":"mcm","mc":[{"marketDefinition":{"eventId":"34773181","openDate":"2025-09-26T00:40:00.000Z"}}]}`,
			expected: &EventInfo{
				EventID: "34773181",
				Year:    "2025",
				Month:   "Sep",
				Day:     "26",
			},
		},
		{
			name:    "Missing event ID",
			json:    `{"op":"mcm","mc":[{"marketDefinition":{"openDate":"2025-09-26T00:40:00.000Z"}}]}`,
			wantErr: true,
		},
		{
			name:    "Empty MC array",
			json:    `{"op":"mcm","mc":[]}`,
			wantErr: true,
		},
		{
			name:    "Invalid JSON",
			json:    `{invalid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ExtractEventInfo([]byte(tt.json))

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.EventID != tt.expected.EventID {
				t.Errorf("Expected EventID '%s', got '%s'", tt.expected.EventID, result.EventID)
			}
			if result.Year != tt.expected.Year {
				t.Errorf("Expected Year '%s', got '%s'", tt.expected.Year, result.Year)
			}
			if result.Month != tt.expected.Month {
				t.Errorf("Expected Month '%s', got '%s'", tt.expected.Month, result.Month)
			}
			if result.Day != tt.expected.Day {
				t.Errorf("Expected Day '%s', got '%s'", tt.expected.Day, result.Day)
			}
		})
	}
}

func TestRemoveIDField(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Remove ID field",
			input:    `{"op":"mcm","id":3,"clk":"test"}`,
			expected: `{"clk":"test","op":"mcm"}`,
		},
		{
			name:     "No ID field",
			input:    `{"op":"mcm","clk":"test"}`,
			expected: `{"clk":"test","op":"mcm"}`,
		},
		{
			name:     "Only ID field",
			input:    `{"id":3}`,
			expected: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := RemoveIDField([]byte(tt.input))
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Parse both to compare JSON content, ignoring key order
			var expected, actual map[string]interface{}
			if err := json.Unmarshal([]byte(tt.expected), &expected); err != nil {
				t.Fatalf("Failed to parse expected JSON: %v", err)
			}
			if err := json.Unmarshal(result, &actual); err != nil {
				t.Fatalf("Failed to parse actual JSON: %v", err)
			}

			if len(expected) != len(actual) {
				t.Errorf("Expected %d fields, got %d", len(expected), len(actual))
			}

			for key, expectedValue := range expected {
				if actualValue, exists := actual[key]; !exists {
					t.Errorf("Missing key '%s'", key)
				} else if expectedValue != actualValue {
					t.Errorf("Key '%s': expected %v, got %v", key, expectedValue, actualValue)
				}
			}
		})
	}
}

func TestExtractAndStoreClock(t *testing.T) {
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
			expectedClk:     "",
		},
		{
			name:            "Only regular clock",
			json:            `{"clk":"clk456"}`,
			expectedInitClk: "",
			expectedClk:     "clk456",
		},
		{
			name:            "No clocks",
			json:            `{"op":"mcm"}`,
			expectedInitClk: "",
			expectedClk:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initialClk, clk := ExtractAndStoreClock([]byte(tt.json))

			if initialClk != tt.expectedInitClk {
				t.Errorf("Expected initialClk '%s', got '%s'", tt.expectedInitClk, initialClk)
			}
			if clk != tt.expectedClk {
				t.Errorf("Expected clk '%s', got '%s'", tt.expectedClk, clk)
			}
		})
	}
}

func TestExtractChangeType(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string
	}{
		{
			name:     "SUB_IMAGE change type",
			json:     `{"op":"mcm","ct":"SUB_IMAGE"}`,
			expected: "SUB_IMAGE",
		},
		{
			name:     "HEARTBEAT change type",
			json:     `{"op":"mcm","ct":"HEARTBEAT"}`,
			expected: "HEARTBEAT",
		},
		{
			name:     "UPDATE change type",
			json:     `{"op":"mcm","ct":"UPDATE"}`,
			expected: "UPDATE",
		},
		{
			name:     "No change type field",
			json:     `{"op":"mcm"}`,
			expected: "",
		},
		{
			name:     "Invalid JSON",
			json:     `{invalid}`,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractChangeType([]byte(tt.json))
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// Benchmark tests
func BenchmarkExtractOp(b *testing.B) {
	payload := []byte(`{"op":"mcm","id":3,"clk":"test"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractOp(payload)
	}
}

func BenchmarkExtractMarketID(b *testing.B) {
	payload := []byte(`{"op":"mcm","mc":[{"id":"1.248231892"}]}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractMarketID(payload)
	}
}

func BenchmarkRemoveIDField(b *testing.B) {
	payload := []byte(`{"op":"mcm","id":3,"clk":"test","mc":[{"id":"1.248231892"}]}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RemoveIDField(payload)
	}
}

// Integration test helper functions
func createTestMarketMessageHelper(marketID, status string) []byte {
	msg := map[string]interface{}{
		"op": "mcm",
		"mc": []map[string]interface{}{
			{
				"id": marketID,
				"marketDefinition": map[string]interface{}{
					"status":   status,
					"eventId":  "34773181",
					"openDate": "2025-09-26T00:40:00.000Z",
				},
			},
		},
		"clk": "test-clock-" + marketID,
	}

	data, _ := json.Marshal(msg)
	return data
}

func TestMarketLifecycle(t *testing.T) {
	// This test simulates a complete market lifecycle
	marketStatuses := make(map[string]string)

	// Market starts as OPEN
	openMsg := createTestMarketMessageHelper("1.test123", "OPEN")
	initialClk, clk := ExtractAndStoreClock(openMsg)

	if clk != "test-clock-1.test123" {
		t.Errorf("Expected clock to be stored as 'test-clock-1.test123', got '%s'", clk)
	}

	// Simulate status change to CLOSED
	marketStatuses["1.test123"] = "OPEN"

	closedMsg := createTestMarketMessageHelper("1.test123", "CLOSED")
	newStatus := ExtractMarketStatus(closedMsg)
	oldStatus := marketStatuses["1.test123"]

	if !IsMarketSettled(newStatus) {
		t.Error("Market should be settled when status is CLOSED")
	}

	if IsMarketSettled(oldStatus) {
		t.Error("Market should not have been settled when status was OPEN")
	}

	// Verify settlement detection logic
	justSettled := !IsMarketSettled(oldStatus) && IsMarketSettled(newStatus)
	if !justSettled {
		t.Error("Should detect that market just became settled")
	}

	// Verify we still have initial clock from first message
	if initialClk != "" {
		t.Logf("Initial clock preserved: %s", initialClk)
	}
}