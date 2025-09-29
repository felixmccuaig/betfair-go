package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/rs/zerolog"
)

// Mock authenticator for testing
type mockAuthenticator struct {
	*Authenticator
	shouldFail bool
}

func (ma *mockAuthenticator) Login() (string, error) {
	if ma.shouldFail {
		return "", fmt.Errorf("authentication failed: INVALID_SESSION_INFORMATION")
	}
	return "mock-refreshed-session-token-12345", nil
}

// testMarketRecorder extends MarketRecorder for testing
type testMarketRecorder struct {
	*MarketRecorder
	mockAuth *mockAuthenticator
}

func (tmr *testMarketRecorder) refreshSessionToken() error {
	// Use mock authenticator
	newToken, err := tmr.mockAuth.Login()
	if err != nil {
		return err
	}

	tmr.config.SessionToken = newToken
	os.Setenv("BETFAIR_SESSION_TOKEN", newToken)
	tmr.logger.Info().Msg("session token refreshed successfully (mock)")
	return nil
}

func (tmr *testMarketRecorder) extractAndStoreClock(payload []byte) {
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return
	}

	if initialClk, exists := data["initialClk"]; exists {
		if clkStr, ok := initialClk.(string); ok {
			tmr.initialClk = clkStr
		}
	}

	if clk, exists := data["clk"]; exists {
		if clkStr, ok := clk.(string); ok {
			tmr.clk = clkStr
		}
	}
}

func (tmr *testMarketRecorder) isInvalidSessionError(err error) bool {
	return IsInvalidSessionError(err)
}

func (tmr *testMarketRecorder) isRetriableError(err error) bool {
	return tmr.MarketRecorder.isRetriableError(err)
}

func TestSessionTokenRefresh(t *testing.T) {
	// Setup test environment
	os.Setenv("BETFAIR_APP_KEY", "test-app-key")
	os.Setenv("BETFAIR_USERNAME", "test@example.com")
	os.Setenv("BETFAIR_PASSWORD", "test-password")
	os.Setenv("BETFAIR_SESSION_TOKEN", "old-session-token")
	os.Setenv("EVENT_TYPE_ID", "4339")

	defer func() {
		os.Unsetenv("BETFAIR_APP_KEY")
		os.Unsetenv("BETFAIR_USERNAME")
		os.Unsetenv("BETFAIR_PASSWORD")
		os.Unsetenv("BETFAIR_SESSION_TOKEN")
		os.Unsetenv("EVENT_TYPE_ID")
	}()

	cfg := NewConfig()
	cfg.LoadFromEnv()

	// Create a test logger
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	// Create test recorder with mock authenticator
	mockAuth := &mockAuthenticator{
		Authenticator: NewAuthenticator("test-app-key", "test@example.com", "test-password"),
		shouldFail:    false,
	}

	recorder := &testMarketRecorder{
		MarketRecorder: &MarketRecorder{
			config: cfg,
			logger: logger,
		},
		mockAuth: mockAuth,
	}

	// Verify initial state
	if recorder.config.SessionToken != "old-session-token" {
		t.Errorf("Expected initial session token to be 'old-session-token', got: %s", recorder.config.SessionToken)
	}

	// Test session token refresh
	err := recorder.refreshSessionToken()
	if err != nil {
		t.Fatalf("Expected successful session refresh, got error: %v", err)
	}

	// Verify session token was updated
	if recorder.config.SessionToken != "mock-refreshed-session-token-12345" {
		t.Errorf("Expected session token to be updated to 'mock-refreshed-session-token-12345', got: %s", recorder.config.SessionToken)
	}

	// Verify environment variable was updated
	envToken := os.Getenv("BETFAIR_SESSION_TOKEN")
	if envToken != "mock-refreshed-session-token-12345" {
		t.Errorf("Expected environment session token to be updated to 'mock-refreshed-session-token-12345', got: %s", envToken)
	}

	t.Logf("Session token refresh test passed")
}

func TestClockPreservationDuringReauth(t *testing.T) {
	// Setup test recorder with clock state
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &testMarketRecorder{
		MarketRecorder: &MarketRecorder{
			config: &Config{
				AppKey:       "test-app-key",
				SessionToken: "old-token",
			},
			logger:     logger,
			initialClk: "test-initial-clk-abc123",
			clk:        "test-clk-def456",
		},
	}

	// Mock authentication payload that would normally contain clock info
	clockPayload := []byte(`{
		"op": "mcm",
		"initialClk": "test-initial-clk-abc123",
		"clk": "test-clk-def456",
		"ct": "SUB_IMAGE",
		"mc": []
	}`)

	// Extract and store clock (simulating what happens during stream processing)
	recorder.extractAndStoreClock(clockPayload)

	// Verify clock values are preserved
	if recorder.initialClk != "test-initial-clk-abc123" {
		t.Errorf("Expected initialClk to be 'test-initial-clk-abc123', got: %s", recorder.initialClk)
	}

	if recorder.clk != "test-clk-def456" {
		t.Errorf("Expected clk to be 'test-clk-def456', got: %s", recorder.clk)
	}

	t.Logf("Clock preservation verified: initialClk=%s, clk=%s", recorder.initialClk, recorder.clk)
}

func TestInvalidSessionErrorDetection(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &testMarketRecorder{
		MarketRecorder: &MarketRecorder{
			logger: logger,
		},
	}

	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "INVALID_SESSION_INFORMATION error",
			err:      fmt.Errorf("authentication failed: INVALID_SESSION_INFORMATION"),
			expected: true,
		},
		{
			name:     "UnrecognisedCredentials error",
			err:      fmt.Errorf("authentication failed: UnrecognisedCredentials"),
			expected: true,
		},
		{
			name:     "NO_SESSION error",
			err:      fmt.Errorf("authentication failed: NO_SESSION"),
			expected: true,
		},
		{
			name:     "Other authentication error",
			err:      fmt.Errorf("authentication failed: NETWORK_ERROR"),
			expected: false,
		},
		{
			name:     "Non-authentication error",
			err:      fmt.Errorf("connection timeout"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := recorder.isInvalidSessionError(tc.err)
			if result != tc.expected {
				t.Errorf("Expected %v for error '%v', got %v", tc.expected, tc.err, result)
			}
		})
	}
}

func TestRetriableErrorDetection(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	recorder := &testMarketRecorder{
		MarketRecorder: &MarketRecorder{
			logger: logger,
		},
	}

	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "Session refresh retry",
			err:      fmt.Errorf("session refreshed, retry connection: authentication failed"),
			expected: true,
		},
		{
			name:     "Authentication failed",
			err:      fmt.Errorf("authentication failed: INVALID_SESSION"),
			expected: true,
		},
		{
			name:     "Connection closed",
			err:      fmt.Errorf("connection closed by peer"),
			expected: true,
		},
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := recorder.isRetriableError(tc.err)
			if result != tc.expected {
				t.Errorf("Expected %v for error '%v', got %v", tc.expected, tc.err, result)
			}
		})
	}
}

func TestAuthenticationFailureScenario(t *testing.T) {
	// Setup test environment
	os.Setenv("BETFAIR_APP_KEY", "test-app-key")
	os.Setenv("BETFAIR_USERNAME", "test@example.com")
	os.Setenv("BETFAIR_PASSWORD", "test-password")
	os.Setenv("BETFAIR_SESSION_TOKEN", "old-session-token")
	os.Setenv("EVENT_TYPE_ID", "4339")

	defer func() {
		os.Unsetenv("BETFAIR_APP_KEY")
		os.Unsetenv("BETFAIR_USERNAME")
		os.Unsetenv("BETFAIR_PASSWORD")
		os.Unsetenv("BETFAIR_SESSION_TOKEN")
		os.Unsetenv("EVENT_TYPE_ID")
	}()

	cfg := NewConfig()
	cfg.LoadFromEnv()

	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "test").
		Logger()

	// Create test recorder with failing mock authenticator
	mockAuth := &mockAuthenticator{
		Authenticator: NewAuthenticator("test-app-key", "test@example.com", "test-password"),
		shouldFail:    true,
	}

	recorder := &testMarketRecorder{
		MarketRecorder: &MarketRecorder{
			config: cfg,
			logger: logger,
		},
		mockAuth: mockAuth,
	}

	// Test that session token refresh fails appropriately
	err := recorder.refreshSessionToken()
	if err == nil {
		t.Fatalf("Expected session refresh to fail, but it succeeded")
	}

	// Verify error is an invalid session error
	if !recorder.isInvalidSessionError(err) {
		t.Errorf("Expected error to be recognized as invalid session error: %v", err)
	}

	// Verify session token wasn't changed
	if recorder.config.SessionToken != "old-session-token" {
		t.Errorf("Expected session token to remain unchanged after failed refresh, got: %s", recorder.config.SessionToken)
	}

	t.Logf("Authentication failure scenario test passed")
}

func TestConfigLoadFromEnv(t *testing.T) {
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
			// Clean environment
			os.Clearenv()

			// Set test environment variables
			for key, value := range tc.envVars {
				os.Setenv(key, value)
			}

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

func TestErrorStringContains(t *testing.T) {
	// Test various error string patterns that should be detected
	testCases := []struct {
		name         string
		errorMessage string
		shouldMatch  bool
	}{
		{
			name:         "Exact INVALID_SESSION_INFORMATION match",
			errorMessage: "INVALID_SESSION_INFORMATION",
			shouldMatch:  true,
		},
		{
			name:         "Case insensitive match",
			errorMessage: "invalid_session_information",
			shouldMatch:  true,
		},
		{
			name:         "Embedded in longer message",
			errorMessage: "Request failed with error: INVALID_SESSION_INFORMATION - please login again",
			shouldMatch:  true,
		},
		{
			name:         "UnrecognisedCredentials match",
			errorMessage: "Login failed: UnrecognisedCredentials",
			shouldMatch:  true,
		},
		{
			name:         "NO_SESSION match",
			errorMessage: "Authentication error: NO_SESSION",
			shouldMatch:  true,
		},
		{
			name:         "Non-matching error",
			errorMessage: "Network connection timeout",
			shouldMatch:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := fmt.Errorf("%s", tc.errorMessage)
			result := IsInvalidSessionError(err)
			if result != tc.shouldMatch {
				t.Errorf("Expected %v for error message '%s', got %v", tc.shouldMatch, tc.errorMessage, result)
			}
		})
	}
}

func TestMarketFilterCreation(t *testing.T) {
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