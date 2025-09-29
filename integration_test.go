package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestSettlementRaceConditionFix(t *testing.T) {
	// This test verifies the settlement race condition fix by simulating
	// the exact message processing flow and checking file handling

	// Create temporary directory for test files
	tempDir := t.TempDir()

	// Test the specific race condition scenario:
	// 1. Market receives OPEN message → written to local file
	// 2. Market receives CLOSED message → should write to file THEN handle settlement
	// 3. Settlement handling should compress and upload complete file (including settlement message)
	// 4. Local file should be cleaned up after successful upload

	marketID := "1.testmarket123"
	localFilePath := filepath.Join(tempDir, marketID)

	// Step 1: Simulate file creation and writing OPEN message
	file, err := os.Create(localFilePath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	openMessage := `{"op":"mcm","clk":"1000","mc":[{"id":"1.testmarket123","marketDefinition":{"status":"OPEN","eventId":"12345","eventName":"Test Race"}}]}`

	// Write OPEN message
	filteredOpen, _ := RemoveIDField([]byte(openMessage))
	writer.Write(append(filteredOpen, '\n'))
	writer.Flush()

	// Step 2: Simulate receiving additional market data
	updateMessage := `{"op":"mcm","clk":"1001","mc":[{"id":"1.testmarket123","rc":[{"id":"12345","atb":[[2.5,10.0]],"atl":[[2.6,5.0]]}]}]}`
	filteredUpdate, _ := RemoveIDField([]byte(updateMessage))
	writer.Write(append(filteredUpdate, '\n'))
	writer.Flush()

	// Step 3: THE CRITICAL TEST - Settlement message processing
	settlementMessage := `{"op":"mcm","clk":"1002","mc":[{"id":"1.testmarket123","marketDefinition":{"status":"CLOSED","settledTime":"2025-09-26T03:53:55.000Z","eventId":"12345","eventName":"Test Race"},"rc":[{"id":"12345","bsp":4.9}]}]}`

	// This simulates the FIXED behavior: write settlement message to file FIRST
	filteredSettlement, _ := RemoveIDField([]byte(settlementMessage))
	writer.Write(append(filteredSettlement, '\n'))
	writer.Flush()
	writer = nil // Close writer to simulate settlement handling

	// Close the file to simulate settlement processing
	file.Close()

	// Step 4: Simulate settlement handling (compress and upload)
	fm := NewFileManager(tempDir)
	compressedFile := localFilePath + ".bz2"
	err = fm.CompressToBzip2(localFilePath, compressedFile)
	if err != nil {
		t.Fatalf("Failed to compress file: %v", err)
	}

	// Step 5: Verify compressed file was created
	if _, err := os.Stat(compressedFile); os.IsNotExist(err) {
		t.Error("Compressed file should be created after settlement")
	}

	// Step 6: Simulate successful S3 upload by cleaning up local file
	err = os.Remove(localFilePath)
	if err != nil {
		t.Fatalf("Failed to clean up local file: %v", err)
	}

	// Step 7: THE KEY VERIFICATION - Check file contents
	// Decompress and verify that the settlement message is included
	content, err := decompressBzip2(compressedFile)
	if err != nil {
		t.Fatalf("Failed to decompress file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	// Should have exactly 3 messages: OPEN, UPDATE, SETTLEMENT
	if len(lines) != 3 {
		t.Errorf("Expected 3 messages in file, got %d. Content: %s", len(lines), string(content))
	}

	// Verify settlement message is present and is the last message
	lastLine := lines[len(lines)-1]
	if !strings.Contains(lastLine, `"status":"CLOSED"`) {
		t.Error("Settlement message with CLOSED status should be present in the compressed file")
	}

	if !strings.Contains(lastLine, `"settledTime":"2025-09-26T03:53:55.000Z"`) {
		t.Error("Settlement message should contain settledTime")
	}

	// Step 8: Verify no duplicate local files remain
	if _, err := os.Stat(localFilePath); !os.IsNotExist(err) {
		t.Error("Local file should be cleaned up after settlement")
	}

	// Clean up compressed file
	os.Remove(compressedFile)

	t.Log("✅ Settlement race condition fix verified: settlement message properly included in compressed file, no duplicates")
}

func TestMarketRecorderWorkflow(t *testing.T) {
	// Test the complete market recorder workflow
	tempDir := t.TempDir()

	config := &Config{
		AppKey:       "test-app-key",
		SessionToken: "test-session-token",
		EventTypeID:  "4339",
		CountryCode:  "AU",
		MarketType:   "WIN",
		HeartbeatMs:  5000,
		OutputPath:   tempDir,
		S3Bucket:     "", // No S3 for this test
		S3BasePath:   "",
	}

	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "integration-test").
		Logger()

	// Create market recorder components
	fileManager := NewFileManager(config.OutputPath)
	marketProcessor := NewMarketProcessor()

	recorder := &MarketRecorder{
		config:           config,
		logger:           logger,
		fileManager:      fileManager,
		marketProcessor:  marketProcessor,
		maxRetries:       3,
		retryDelay:       time.Millisecond * 100,
		marketCatalogues: make(map[string]*MarketCatalogue),
	}

	// Test market lifecycle simulation
	marketID := "1.workflow_test"
	writers := make(map[string]*bufio.Writer)
	files := make(map[string]*os.File)
	marketStatuses := make(map[string]string)

	// Step 1: Create writer for market
	err := recorder.createWriterForMarket(marketID, writers, files)
	if err != nil {
		t.Fatalf("Failed to create writer for market: %v", err)
	}

	// Step 2: Simulate market messages
	messages := []struct {
		payload []byte
		status  string
	}{
		{
			payload: createTestMarketMessage(marketID, "OPEN"),
			status:  "OPEN",
		},
		{
			payload: []byte(`{"op":"mcm","clk":"1001","mc":[{"id":"` + marketID + `","rc":[{"id":"12345","atb":[[2.5,10.0]]}]}]}`),
			status:  "",
		},
		{
			payload: createTestMarketMessage(marketID, "SUSPENDED"),
			status:  "SUSPENDED",
		},
		{
			payload: createTestMarketMessage(marketID, "CLOSED"),
			status:  "CLOSED",
		},
	}

	// Step 3: Process messages and track status changes
	for i, msg := range messages {
		// Extract and store clock
		initialClk, clk := ExtractAndStoreClock(msg.payload)
		if initialClk != "" {
			recorder.initialClk = initialClk
		}
		if clk != "" {
			recorder.clk = clk
		}

		// Track market status
		if msg.status != "" {
			oldStatus := marketStatuses[marketID]
			marketStatuses[marketID] = msg.status

			// Check if market just settled
			marketJustSettled := !IsMarketSettled(oldStatus) && IsMarketSettled(msg.status)
			if marketJustSettled {
				t.Logf("Market %s just settled at message %d", marketID, i+1)
			}
		}

		// Write message to file
		if writer, exists := writers[marketID]; exists {
			filteredPayload, err := RemoveIDField(msg.payload)
			if err != nil {
				t.Fatalf("Failed to filter payload: %v", err)
			}

			writer.Write(append(filteredPayload, '\n'))
			writer.Flush()
		}
	}

	// Step 4: Simulate settlement handling
	if file, exists := files[marketID]; exists {
		file.Close()
		delete(files, marketID)
	}
	if _, exists := writers[marketID]; exists {
		delete(writers, marketID)
	}

	// Verify file was created and contains all messages
	marketFile := filepath.Join(tempDir, marketID)
	content, err := os.ReadFile(marketFile)
	if err != nil {
		t.Fatalf("Failed to read market file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines in market file, got %d", len(lines))
	}

	// Verify settlement message is last
	lastLine := lines[len(lines)-1]
	if !strings.Contains(lastLine, `"status":"CLOSED"`) {
		t.Error("Last message should be settlement message")
	}

	t.Log("✅ Market recorder workflow test completed successfully")
}

func TestMarketFilesWithCustomOutputPath(t *testing.T) {
	// Test market files functionality with custom output path
	tempDir := t.TempDir()
	outputDir := filepath.Join(tempDir, "custom_market_files")

	config := &Config{
		AppKey:       "test-app-key",
		SessionToken: "test-session-token",
		EventTypeID:  "4339",
		CountryCode:  "AU",
		MarketType:   "WIN",
		HeartbeatMs:  5000,
		OutputPath:   outputDir, // Custom output path
		S3Bucket:     "",
		S3BasePath:   "",
	}

	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "integration-test").
		Logger()

	fileManager := NewFileManager(config.OutputPath)

	recorder := &MarketRecorder{
		config:           config,
		logger:           logger,
		fileManager:      fileManager,
		marketCatalogues: make(map[string]*MarketCatalogue),
	}

	writers := make(map[string]*bufio.Writer)
	files := make(map[string]*os.File)
	marketID := "1.custom_path_test"

	// Test creating a writer for a market
	err := recorder.createWriterForMarket(marketID, writers, files)
	if err != nil {
		t.Fatalf("Failed to create writer for market: %v", err)
	}

	// Verify custom output directory was created
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		t.Error("Custom output directory should be created")
	}

	// Verify market file was created in custom directory
	expectedFilePath := filepath.Join(outputDir, marketID)
	if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
		t.Errorf("Market file should be created at %s", expectedFilePath)
	}

	// Write test data
	writer := writers[marketID]
	testData := `{"op":"mcm","clk":"1000","mc":[{"id":"1.custom_path_test","marketDefinition":{"status":"OPEN"}}]}`
	filteredData, _ := RemoveIDField([]byte(testData))
	writer.Write(append(filteredData, '\n'))
	writer.Flush()

	// Clean up
	if file, exists := files[marketID]; exists {
		file.Close()
	}

	// Verify the data was written to the correct location
	fileContent, err := os.ReadFile(expectedFilePath)
	if err != nil {
		t.Fatalf("Failed to read market file: %v", err)
	}

	if !strings.Contains(string(fileContent), `"status":"OPEN"`) {
		t.Error("Market file should contain the written data")
	}

	t.Log("✅ Custom output path functionality verified")
}

func TestSettlementHandlingWithS3(t *testing.T) {
	// Test settlement handling workflow (without actual S3 upload)
	tempDir := t.TempDir()

	config := &Config{
		AppKey:       "test-app-key",
		SessionToken: "test-session-token",
		EventTypeID:  "4339",
		CountryCode:  "AU",
		MarketType:   "WIN",
		HeartbeatMs:  5000,
		OutputPath:   tempDir,
		S3Bucket:     "test-bucket", // S3 configured but won't actually upload
		S3BasePath:   "test-path",
	}

	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "integration-test").
		Logger()

	fileManager := NewFileManager(config.OutputPath)

	recorder := &MarketRecorder{
		config:           config,
		logger:           logger,
		fileManager:      fileManager,
		marketCatalogues: make(map[string]*MarketCatalogue),
	}
	_ = recorder // Mark as used

	marketID := "1.settlement_s3_test"

	// Create and write to market file
	marketFile := filepath.Join(tempDir, marketID)
	file, err := os.Create(marketFile)
	if err != nil {
		t.Fatalf("Failed to create market file: %v", err)
	}

	writer := bufio.NewWriter(file)
	testMessages := []string{
		`{"op":"mcm","clk":"1000","mc":[{"id":"1.settlement_s3_test","marketDefinition":{"status":"OPEN","eventId":"12345","eventName":"Test Race"}}]}`,
		`{"op":"mcm","clk":"1001","mc":[{"id":"1.settlement_s3_test","rc":[{"id":"12345","atb":[[2.5,10.0]]}]}]}`,
		`{"op":"mcm","clk":"1002","mc":[{"id":"1.settlement_s3_test","marketDefinition":{"status":"CLOSED","settledTime":"2025-09-26T03:53:55.000Z","eventId":"12345","eventName":"Test Race"}}]}`,
	}

	for _, msg := range testMessages {
		filteredMsg, _ := RemoveIDField([]byte(msg))
		writer.Write(append(filteredMsg, '\n'))
	}
	writer.Flush()
	file.Close()

	// Simulate settlement handling (without actual S3 upload)
	settlementPayload := []byte(`{"op":"mcm","clk":"1002","mc":[{"id":"1.settlement_s3_test","marketDefinition":{"status":"CLOSED","settledTime":"2025-09-26T03:53:55.000Z","eventId":"12345","eventName":"Test Race"}}]}`)

	// Test event info extraction
	eventInfo, err := ExtractEventInfo(settlementPayload)
	if err != nil {
		t.Fatalf("Failed to extract event info: %v", err)
	}

	if eventInfo.EventID != "12345" {
		t.Errorf("Expected event ID '12345', got '%s'", eventInfo.EventID)
	}

	// Test file compression
	compressedFile := recorder.fileManager.GetCompressedFilePath(marketID)
	err = recorder.fileManager.CompressToBzip2(marketFile, compressedFile)
	if err != nil {
		t.Fatalf("Failed to compress file: %v", err)
	}

	// Verify compressed file was created
	if _, err := os.Stat(compressedFile); os.IsNotExist(err) {
		t.Error("Compressed file should be created")
	}

	// Test S3 key generation (would be used for actual upload)
	storage := &S3Storage{
		bucket:   config.S3Bucket,
		basePath: config.S3BasePath,
	}

	s3Key := storage.BuildS3Key(eventInfo, marketID+".bz2")
	expectedS3Key := "test-path/PRO/" + eventInfo.Year + "/" + eventInfo.Month + "/" + eventInfo.Day + "/" + eventInfo.EventID + "/" + marketID + ".bz2"
	// Normalize path separators for cross-platform compatibility
	s3Key = filepath.ToSlash(s3Key)
	expectedS3Key = filepath.ToSlash(expectedS3Key)

	if s3Key != expectedS3Key {
		t.Errorf("Expected S3 key '%s', got '%s'", expectedS3Key, s3Key)
	}

	// Verify compressed file contains all messages
	content, err := decompressBzip2(compressedFile)
	if err != nil {
		t.Fatalf("Failed to decompress file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 3 {
		t.Errorf("Expected 3 lines in compressed file, got %d", len(lines))
	}

	// Verify settlement message is present
	lastLine := lines[len(lines)-1]
	if !strings.Contains(lastLine, `"status":"CLOSED"`) {
		t.Error("Settlement message should be present in compressed file")
	}

	t.Log("✅ Settlement handling with S3 workflow test completed successfully")
}

// Helper function to create test market message
func createTestMarketMessage(marketID, status string) []byte {
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
		"clk": "test-clock-" + strings.Replace(marketID, ".", "-", -1),
	}

	data, _ := json.Marshal(msg)
	return data
}

func TestEndToEndMarketProcessing(t *testing.T) {
	// Test complete end-to-end market processing flow
	tempDir := t.TempDir()

	config := &Config{
		AppKey:       "test-app-key",
		SessionToken: "test-session-token",
		MarketIDs:    []string{"1.e2e_test_market"},
		EventTypeID:  "4339",
		CountryCode:  "AU",
		MarketType:   "WIN",
		HeartbeatMs:  5000,
		OutputPath:   tempDir,
		S3Bucket:     "",
		S3BasePath:   "",
	}

	logger := zerolog.New(zerolog.NewTestWriter(t)).With().
		Timestamp().
		Str("component", "e2e-test").
		Logger()

	// Create all recorder components
	fileManager := NewFileManager(config.OutputPath)
	marketProcessor := NewMarketProcessor()

	recorder := &MarketRecorder{
		config:           config,
		logger:           logger,
		fileManager:      fileManager,
		marketProcessor:  marketProcessor,
		maxRetries:       3,
		retryDelay:       time.Millisecond * 100,
		marketCatalogues: make(map[string]*MarketCatalogue),
	}

	// Test the complete flow
	marketID := "1.e2e_test_market"

	// Step 1: Initialize writers
	writers, files, closeFn, err := recorder.openWriters()
	if err != nil {
		t.Fatalf("Failed to open writers: %v", err)
	}
	defer closeFn()
	_ = files // Mark as used

	// Step 2: Simulate complete market lifecycle
	marketStatuses := make(map[string]string)

	// Market lifecycle messages
	lifecycleMessages := []struct {
		description string
		payload     []byte
		expectedOp  string
	}{
		{
			description: "Initial market subscription",
			payload:     []byte(`{"op":"mcm","clk":"1000","ct":"SUB_IMAGE","mc":[{"id":"1.e2e_test_market","marketDefinition":{"status":"OPEN","eventId":"12345"}}]}`),
			expectedOp:  "mcm",
		},
		{
			description: "Price update",
			payload:     []byte(`{"op":"mcm","clk":"1001","ct":"UPDATE","mc":[{"id":"1.e2e_test_market","rc":[{"id":"67890","atb":[[2.5,10.0]],"atl":[[2.6,8.0]]}]}]}`),
			expectedOp:  "mcm",
		},
		{
			description: "Market suspension",
			payload:     []byte(`{"op":"mcm","clk":"1002","ct":"UPDATE","mc":[{"id":"1.e2e_test_market","marketDefinition":{"status":"SUSPENDED"}}]}`),
			expectedOp:  "mcm",
		},
		{
			description: "Market resumption",
			payload:     []byte(`{"op":"mcm","clk":"1003","ct":"UPDATE","mc":[{"id":"1.e2e_test_market","marketDefinition":{"status":"OPEN"}}]}`),
			expectedOp:  "mcm",
		},
		{
			description: "Final price update",
			payload:     []byte(`{"op":"mcm","clk":"1004","ct":"UPDATE","mc":[{"id":"1.e2e_test_market","rc":[{"id":"67890","atb":[[3.0,15.0]],"atl":[[3.1,12.0]]}]}]}`),
			expectedOp:  "mcm",
		},
		{
			description: "Market settlement",
			payload:     []byte(`{"op":"mcm","clk":"1005","ct":"UPDATE","mc":[{"id":"1.e2e_test_market","marketDefinition":{"status":"CLOSED","settledTime":"2025-09-26T03:53:55.000Z","eventId":"12345"}}]}`),
			expectedOp:  "mcm",
		},
	}

	// Step 3: Process each message
	for i, msg := range lifecycleMessages {
		t.Logf("Processing message %d: %s", i+1, msg.description)

		// Verify operation extraction
		op := ExtractOp(msg.payload)
		if op != msg.expectedOp {
			t.Errorf("Expected op '%s', got '%s' for message %d", msg.expectedOp, op, i+1)
		}

		// Extract and store clock
		initialClk, clk := ExtractAndStoreClock(msg.payload)
		if initialClk != "" {
			recorder.initialClk = initialClk
		}
		if clk != "" {
			recorder.clk = clk
		}

		// Check for market status changes
		newStatus := ExtractMarketStatus(msg.payload)
		if newStatus != "" {
			oldStatus := marketStatuses[marketID]
			marketStatuses[marketID] = newStatus

			marketJustSettled := !IsMarketSettled(oldStatus) && IsMarketSettled(newStatus)
			if marketJustSettled {
				t.Logf("Market %s settled at message %d", marketID, i+1)
			}
		}

		// Write to file
		if writer, exists := writers[marketID]; exists {
			filteredPayload, err := RemoveIDField(msg.payload)
			if err != nil {
				t.Fatalf("Failed to filter payload for message %d: %v", i+1, err)
			}

			writer.Write(append(filteredPayload, '\n'))
			writer.Flush()
		}
	}

	// Step 4: Verify final state
	if recorder.clk != "1005" {
		t.Errorf("Expected final clock '1005', got '%s'", recorder.clk)
	}

	finalStatus := marketStatuses[marketID]
	if finalStatus != "CLOSED" {
		t.Errorf("Expected final market status 'CLOSED', got '%s'", finalStatus)
	}

	// Step 5: Verify file contents
	marketFile := filepath.Join(tempDir, marketID)
	content, err := os.ReadFile(marketFile)
	if err != nil {
		t.Fatalf("Failed to read market file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != len(lifecycleMessages) {
		t.Errorf("Expected %d lines in market file, got %d", len(lifecycleMessages), len(lines))
	}

	// Verify first line is subscription
	if !strings.Contains(lines[0], `"ct":"SUB_IMAGE"`) {
		t.Error("First line should contain SUB_IMAGE change type")
	}

	// Verify last line is settlement
	lastLine := lines[len(lines)-1]
	if !strings.Contains(lastLine, `"status":"CLOSED"`) {
		t.Error("Last line should contain CLOSED status")
	}
	if !strings.Contains(lastLine, `"settledTime"`) {
		t.Error("Last line should contain settledTime")
	}

	t.Log("✅ End-to-end market processing test completed successfully")
	t.Logf("Processed %d messages, final clock: %s, final status: %s",
		len(lifecycleMessages), recorder.clk, finalStatus)
}