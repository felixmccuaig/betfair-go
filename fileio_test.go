package betfair

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dsnet/compress/bzip2"
)

func TestFileManagerCreateMarketWriter(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()

	// Test with custom output path
	fm := NewFileManager(tempDir)
	marketID := "1.testmarket123"

	writer, file, err := fm.CreateMarketWriter(marketID)
	if err != nil {
		t.Fatalf("Failed to create market writer: %v", err)
	}
	defer file.Close()

	// Verify the file was created in the correct location
	expectedPath := filepath.Join(tempDir, marketID)
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Market file should be created at %s", expectedPath)
	}

	// Test writing to the file
	testData := "test market data\n"
	writer.WriteString(testData)
	writer.Flush()

	// Read back the data
	content, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read market file: %v", err)
	}

	if string(content) != testData {
		t.Errorf("Expected '%s', got '%s'", testData, string(content))
	}
}

func TestFileManagerDefaultOutputPath(t *testing.T) {
	// Change to temporary directory to avoid creating files in the repo
	originalWd, _ := os.Getwd()
	tempDir := t.TempDir()
	os.Chdir(tempDir)
	defer os.Chdir(originalWd)

	// Test with default output path (empty string)
	fm := NewFileManager("")
	marketID := "1.testmarket456"

	writer, file, err := fm.CreateMarketWriter(marketID)
	if err != nil {
		t.Fatalf("Failed to create market writer: %v", err)
	}
	defer file.Close()
	_ = writer // Mark as used

	// Verify market_files directory was created
	if _, err := os.Stat("market_files"); os.IsNotExist(err) {
		t.Error("market_files directory should be created with default output path")
	}

	// Verify file was created in market_files directory
	expectedPath := filepath.Join("market_files", marketID)
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Market file should be created at %s", expectedPath)
	}
}

func TestFileManagerGetFilePaths(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)
	marketID := "1.testmarket789"

	// Test GetMarketFilePath
	marketPath := fm.GetMarketFilePath(marketID)
	expectedMarketPath := filepath.Join(tempDir, marketID)
	if marketPath != expectedMarketPath {
		t.Errorf("Expected market path '%s', got '%s'", expectedMarketPath, marketPath)
	}

	// Test GetCompressedFilePath
	compressedPath := fm.GetCompressedFilePath(marketID)
	expectedCompressedPath := filepath.Join(tempDir, marketID+".bz2")
	if compressedPath != expectedCompressedPath {
		t.Errorf("Expected compressed path '%s', got '%s'", expectedCompressedPath, compressedPath)
	}
}

func TestFileManagerCompressToBzip2(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Create a test file with data
	testData := "Line 1: Market data\nLine 2: More market data\nLine 3: Final market data\n"
	inputFile := filepath.Join(tempDir, "test_input.txt")
	err := os.WriteFile(inputFile, []byte(testData), 0644)
	if err != nil {
		t.Fatalf("Failed to create test input file: %v", err)
	}

	// Compress the file
	outputFile := filepath.Join(tempDir, "test_output.bz2")
	err = fm.CompressToBzip2(inputFile, outputFile)
	if err != nil {
		t.Fatalf("Failed to compress file: %v", err)
	}

	// Verify compressed file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Compressed file should be created")
	}

	// Decompress and verify content
	decompressedData, err := decompressBzip2(outputFile)
	if err != nil {
		t.Fatalf("Failed to decompress file for verification: %v", err)
	}

	if string(decompressedData) != testData {
		t.Errorf("Decompressed data doesn't match original.\nExpected: %s\nGot: %s", testData, string(decompressedData))
	}
}

func TestFileManagerCleanupFiles(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Create test files
	file1 := filepath.Join(tempDir, "test1.txt")
	file2 := filepath.Join(tempDir, "test2.txt")
	file3 := filepath.Join(tempDir, "test3.txt")

	for _, file := range []string{file1, file2, file3} {
		err := os.WriteFile(file, []byte("test data"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", file, err)
		}
	}

	// Verify files exist
	for _, file := range []string{file1, file2, file3} {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			t.Errorf("Test file %s should exist before cleanup", file)
		}
	}

	// Clean up files
	fm.CleanupFiles(file1, file2, file3)

	// Verify files are removed
	for _, file := range []string{file1, file2, file3} {
		if _, err := os.Stat(file); !os.IsNotExist(err) {
			t.Errorf("Test file %s should be removed after cleanup", file)
		}
	}
}

func TestFileManagerCleanupNonexistentFiles(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Try to clean up files that don't exist (should not error)
	nonexistentFiles := []string{
		filepath.Join(tempDir, "nonexistent1.txt"),
		filepath.Join(tempDir, "nonexistent2.txt"),
		filepath.Join(tempDir, "nonexistent3.txt"),
	}

	// This should not panic or error
	fm.CleanupFiles(nonexistentFiles...)

	// Test passes if we reach this point without error
	t.Log("Cleanup of nonexistent files completed without error")
}

func TestBuildEventPath(t *testing.T) {
	eventInfo := &EventInfo{
		EventID: "34773181",
		Year:    "2025",
		Month:   "Sep",
		Day:     "26",
	}

	tests := []struct {
		name     string
		basePath string
		expected string
	}{
		{
			name:     "With custom base path",
			basePath: "custom-path",
			expected: "custom-path/PRO/2025/Sep/26/34773181",
		},
		{
			name:     "With empty base path",
			basePath: "",
			expected: "PRO/2025/Sep/26/34773181",
		},
		{
			name:     "With root path",
			basePath: "/data",
			expected: "/data/PRO/2025/Sep/26/34773181",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildEventPath(tt.basePath, eventInfo)
			// Normalize path separators for cross-platform compatibility
			result = filepath.ToSlash(result)
			expected := filepath.ToSlash(tt.expected)
			if result != expected {
				t.Errorf("Expected '%s', got '%s'", expected, result)
			}
		})
	}
}

func TestFileManagerIntegration(t *testing.T) {
	// Test the complete file management workflow
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)
	marketID := "1.integration_test"

	// Step 1: Create a market writer
	writer, file, err := fm.CreateMarketWriter(marketID)
	if err != nil {
		t.Fatalf("Failed to create market writer: %v", err)
	}

	// Step 2: Write market data
	marketData := []string{
		`{"op":"mcm","clk":"1000","mc":[{"id":"1.integration_test","marketDefinition":{"status":"OPEN"}}]}`,
		`{"op":"mcm","clk":"1001","mc":[{"id":"1.integration_test","rc":[{"id":"12345","atb":[[2.5,10.0]]}]}]}`,
		`{"op":"mcm","clk":"1002","mc":[{"id":"1.integration_test","marketDefinition":{"status":"CLOSED"}}]}`,
	}

	for _, data := range marketData {
		writer.WriteString(data + "\n")
	}
	writer.Flush()
	file.Close()

	// Step 3: Get file paths
	inputFile := fm.GetMarketFilePath(marketID)
	outputFile := fm.GetCompressedFilePath(marketID)

	// Step 4: Compress the file
	err = fm.CompressToBzip2(inputFile, outputFile)
	if err != nil {
		t.Fatalf("Failed to compress file: %v", err)
	}

	// Step 5: Verify compressed file contains all data
	decompressedData, err := decompressBzip2(outputFile)
	if err != nil {
		t.Fatalf("Failed to decompress file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(decompressedData)), "\n")
	if len(lines) != 3 {
		t.Errorf("Expected 3 lines in compressed file, got %d", len(lines))
	}

	// Verify specific content
	if !strings.Contains(lines[0], `"status":"OPEN"`) {
		t.Error("First line should contain OPEN status")
	}
	if !strings.Contains(lines[2], `"status":"CLOSED"`) {
		t.Error("Last line should contain CLOSED status")
	}

	// Step 6: Clean up files
	fm.CleanupFiles(inputFile, outputFile)

	// Verify cleanup
	if _, err := os.Stat(inputFile); !os.IsNotExist(err) {
		t.Error("Input file should be cleaned up")
	}
	if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
		t.Error("Output file should be cleaned up")
	}

	t.Log("✅ File management integration test completed successfully")
}

// Helper function to decompress bzip2 file for testing
func decompressBzip2(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader, err := bzip2.NewReader(file, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var result []byte
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
	}
	return result, nil
}

func TestFileManagerWithOutputPathSet(t *testing.T) {
	// Test the exact scenario from user's .env: OUTPUT_PATH=market_files
	// This should create files directly in the market_files directory

	// Change to temporary directory to avoid creating files in the repo
	originalWd, _ := os.Getwd()
	tempDir := t.TempDir()
	os.Chdir(tempDir)
	defer os.Chdir(originalWd)

	// Test FileManager with OUTPUT_PATH=market_files (matching user's .env)
	fm := NewFileManager("market_files")
	marketID := "1.248231131" // Using the market ID from the error

	// Test creating a writer for a market
	writer, file, err := fm.CreateMarketWriter(marketID)
	if err != nil {
		t.Fatalf("Failed to create market writer: %v", err)
	}
	defer file.Close()

	// Verify market_files directory was created
	if _, err := os.Stat("market_files"); os.IsNotExist(err) {
		t.Error("market_files directory should be created")
	}

	// Verify market file was created in market_files directory
	expectedFilePath := filepath.Join("market_files", marketID)
	if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
		t.Errorf("Market file should be created at %s", expectedFilePath)
	}

	// Write some test data
	testData := `{"op":"mcm","clk":"1000","mc":[{"id":"1.248231131","marketDefinition":{"status":"OPEN"}}]}`
	writer.WriteString(testData + "\n")
	writer.Flush()

	// Verify the data was written
	fileContent, err := os.ReadFile(expectedFilePath)
	if err != nil {
		t.Fatalf("Failed to read market file: %v", err)
	}

	if !strings.Contains(string(fileContent), `"status":"OPEN"`) {
		t.Error("Market file should contain the written data")
	}

	t.Log("✅ OUTPUT_PATH=market_files functionality verified: directory auto-created, files saved correctly")
}