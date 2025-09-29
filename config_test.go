package betfair

import (
	"os"
	"testing"
)

func TestConfigLoadFromEnvBasic(t *testing.T) {
	// Save original env vars
	originalAppKey := os.Getenv("BETFAIR_APP_KEY")
	originalSessionToken := os.Getenv("BETFAIR_SESSION_TOKEN")
	originalEventTypeID := os.Getenv("EVENT_TYPE_ID")
	originalCountryCode := os.Getenv("COUNTRY_CODE")
	originalMarketType := os.Getenv("MARKET_TYPE")
	originalHeartbeat := os.Getenv("HEARTBEAT_MS")

	// Clean up after test
	defer func() {
		os.Setenv("BETFAIR_APP_KEY", originalAppKey)
		os.Setenv("BETFAIR_SESSION_TOKEN", originalSessionToken)
		os.Setenv("EVENT_TYPE_ID", originalEventTypeID)
		os.Setenv("COUNTRY_CODE", originalCountryCode)
		os.Setenv("MARKET_TYPE", originalMarketType)
		os.Setenv("HEARTBEAT_MS", originalHeartbeat)
	}()

	// Test with environment variables
	os.Setenv("BETFAIR_APP_KEY", "test-app-key")
	os.Setenv("BETFAIR_SESSION_TOKEN", "test-session-token")
	os.Setenv("EVENT_TYPE_ID", "4339")
	os.Setenv("COUNTRY_CODE", "AU")
	os.Setenv("MARKET_TYPE", "WIN")
	os.Setenv("HEARTBEAT_MS", "3000")

	cfg := NewConfig()
	err := cfg.LoadFromEnv()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.AppKey != "test-app-key" {
		t.Errorf("Expected AppKey 'test-app-key', got '%s'", cfg.AppKey)
	}
	if cfg.EventTypeID != "4339" {
		t.Errorf("Expected EventTypeID '4339', got '%s'", cfg.EventTypeID)
	}
	if cfg.CountryCode != "AU" {
		t.Errorf("Expected CountryCode 'AU', got '%s'", cfg.CountryCode)
	}
	if cfg.MarketType != "WIN" {
		t.Errorf("Expected MarketType 'WIN', got '%s'", cfg.MarketType)
	}
	if cfg.HeartbeatMs != 3000 {
		t.Errorf("Expected HeartbeatMs 3000, got %d", cfg.HeartbeatMs)
	}
}

func TestConfigGetMarketFilter(t *testing.T) {
	cfg := &Config{
		MarketIDs:   []string{"1.12345", "1.67890"},
		EventTypeID: "4339",
		CountryCode: "AU",
		MarketType:  "WIN",
	}

	filter := cfg.GetMarketFilter()

	// Test that market filter is created correctly
	if len(filter.MarketIds) != 2 {
		t.Errorf("Expected 2 market IDs, got %d", len(filter.MarketIds))
	}

	if filter.MarketIds[0] != "1.12345" || filter.MarketIds[1] != "1.67890" {
		t.Errorf("Market IDs not set correctly: %v", filter.MarketIds)
	}

	if len(filter.EventTypeIds) != 1 || filter.EventTypeIds[0] != "4339" {
		t.Errorf("Event type ID not set correctly: %v", filter.EventTypeIds)
	}

	if len(filter.MarketCountries) != 1 || filter.MarketCountries[0] != "AU" {
		t.Errorf("Country code not set correctly: %v", filter.MarketCountries)
	}

	if len(filter.MarketTypeCodes) != 1 || filter.MarketTypeCodes[0] != "WIN" {
		t.Errorf("Market type not set correctly: %v", filter.MarketTypeCodes)
	}

	t.Logf("Market filter creation test passed")
}

func TestSplitAndClean(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Simple comma separated",
			input:    "1.111,1.222,1.333",
			expected: []string{"1.111", "1.222", "1.333"},
		},
		{
			name:     "With spaces",
			input:    "1.111, 1.222 , 1.333",
			expected: []string{"1.111", "1.222", "1.333"},
		},
		{
			name:     "Empty strings",
			input:    "1.111,,1.333,",
			expected: []string{"1.111", "1.333"},
		},
		{
			name:     "Single value",
			input:    "1.111",
			expected: []string{"1.111"},
		},
		{
			name:     "Empty input",
			input:    "",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitAndClean(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected length %d, got %d", len(tt.expected), len(result))
				return
			}

			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("Index %d: expected '%s', got '%s'", i, expected, result[i])
				}
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	// Test configuration loading with different scenarios
	testCases := []struct {
		name     string
		envVars  map[string]string
		expectOk bool
	}{
		{
			name: "Valid configuration with session token",
			envVars: map[string]string{
				"BETFAIR_APP_KEY":      "test-app-key",
				"BETFAIR_SESSION_TOKEN": "existing-token",
				"EVENT_TYPE_ID":        "4339",
			},
			expectOk: true,
		},
		{
			name: "Valid configuration with market IDs",
			envVars: map[string]string{
				"BETFAIR_APP_KEY":      "test-app-key",
				"BETFAIR_SESSION_TOKEN": "existing-token",
				"MARKET_IDS":           "1.12345,1.67890",
			},
			expectOk: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Save current environment
			savedEnv := make(map[string]string)
			for key := range tc.envVars {
				savedEnv[key] = os.Getenv(key)
			}

			// Clean and set test environment variables
			for key := range tc.envVars {
				os.Unsetenv(key)
			}
			for key, value := range tc.envVars {
				os.Setenv(key, value)
			}

			// Restore environment after test
			defer func() {
				for key, value := range savedEnv {
					if value == "" {
						os.Unsetenv(key)
					} else {
						os.Setenv(key, value)
					}
				}
			}()

			// Test configuration loading
			cfg := NewConfig()
			err := cfg.LoadFromEnv()

			if tc.expectOk && err != nil {
				t.Errorf("Expected configuration loading to succeed, got error: %v", err)
			}

			if tc.expectOk {
				// Verify key values are set
				if cfg.AppKey != tc.envVars["BETFAIR_APP_KEY"] {
					t.Errorf("Expected AppKey to be '%s', got '%s'", tc.envVars["BETFAIR_APP_KEY"], cfg.AppKey)
				}
				if cfg.SessionToken != tc.envVars["BETFAIR_SESSION_TOKEN"] {
					t.Errorf("Expected SessionToken to be '%s', got '%s'", tc.envVars["BETFAIR_SESSION_TOKEN"], cfg.SessionToken)
				}
			}
		})
	}
}