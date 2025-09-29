package main

import (
	"path/filepath"
	"testing"
)

func TestS3StorageBuildS3Key(t *testing.T) {
	eventInfo := &EventInfo{
		EventID: "34773181",
		Year:    "2025",
		Month:   "Sep",
		Day:     "26",
	}

	tests := []struct {
		name     string
		basePath string
		filename string
		expected string
	}{
		{
			name:     "With custom base path",
			basePath: "custom-path",
			filename: "1.248231892.bz2",
			expected: "custom-path/PRO/2025/Sep/26/34773181/1.248231892.bz2",
		},
		{
			name:     "With empty base path (uses default)",
			basePath: "",
			filename: "1.248231892.bz2",
			expected: "raw_greyhounds_data/PRO/2025/Sep/26/34773181/1.248231892.bz2",
		},
		{
			name:     "With different filename",
			basePath: "test-data",
			filename: "market.json.bz2",
			expected: "test-data/PRO/2025/Sep/26/34773181/market.json.bz2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := &S3Storage{
				bucket:   "test-bucket",
				basePath: tt.basePath,
			}

			result := storage.BuildS3Key(eventInfo, tt.filename)
			// Normalize path separators for cross-platform compatibility
			result = filepath.ToSlash(result)
			expected := filepath.ToSlash(tt.expected)
			if result != expected {
				t.Errorf("Expected '%s', got '%s'", expected, result)
			}
		})
	}
}

func TestS3StorageDefaultBasePath(t *testing.T) {
	// Test that default base path is used when none is provided
	eventInfo := &EventInfo{
		EventID: "12345",
		Year:    "2024",
		Month:   "Dec",
		Day:     "31",
	}

	storage := &S3Storage{
		bucket:   "test-bucket",
		basePath: "", // Empty base path should trigger default
	}

	result := storage.BuildS3Key(eventInfo, "test.bz2")
	expected := "raw_greyhounds_data/PRO/2024/Dec/31/12345/test.bz2"

	// Normalize path separators for cross-platform compatibility
	result = filepath.ToSlash(result)
	expected = filepath.ToSlash(expected)

	if result != expected {
		t.Errorf("Expected default base path usage: '%s', got '%s'", expected, result)
	}
}

func TestS3StorageKeyWithSpecialCharacters(t *testing.T) {
	// Test S3 key generation with special characters in filename
	eventInfo := &EventInfo{
		EventID: "34773181",
		Year:    "2025",
		Month:   "Sep",
		Day:     "26",
	}

	storage := &S3Storage{
		bucket:   "test-bucket",
		basePath: "test-data",
	}

	tests := []struct {
		name     string
		filename string
		expected string
	}{
		{
			name:     "Standard filename",
			filename: "1.248231892.bz2",
			expected: "test-data/PRO/2025/Sep/26/34773181/1.248231892.bz2",
		},
		{
			name:     "Filename with underscores",
			filename: "market_data_final.bz2",
			expected: "test-data/PRO/2025/Sep/26/34773181/market_data_final.bz2",
		},
		{
			name:     "Filename with dashes",
			filename: "market-data-2025.bz2",
			expected: "test-data/PRO/2025/Sep/26/34773181/market-data-2025.bz2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := storage.BuildS3Key(eventInfo, tt.filename)
			// Normalize path separators for cross-platform compatibility
			result = filepath.ToSlash(result)
			expected := filepath.ToSlash(tt.expected)
			if result != expected {
				t.Errorf("Expected '%s', got '%s'", expected, result)
			}
		})
	}
}

func TestS3StorageKeyConsistency(t *testing.T) {
	// Test that the same inputs always produce the same S3 key
	eventInfo := &EventInfo{
		EventID: "34773181",
		Year:    "2025",
		Month:   "Sep",
		Day:     "26",
	}

	storage := &S3Storage{
		bucket:   "test-bucket",
		basePath: "consistent-test",
	}

	filename := "1.248231892.bz2"

	// Generate the same key multiple times
	key1 := storage.BuildS3Key(eventInfo, filename)
	key2 := storage.BuildS3Key(eventInfo, filename)
	key3 := storage.BuildS3Key(eventInfo, filename)

	if key1 != key2 || key2 != key3 {
		t.Errorf("S3 key generation should be consistent. Got: '%s', '%s', '%s'", key1, key2, key3)
	}

	expectedPattern := "consistent-test/PRO/2025/Sep/26/34773181/1.248231892.bz2"
	// Normalize path separators for cross-platform compatibility
	key1 = filepath.ToSlash(key1)
	expectedPattern = filepath.ToSlash(expectedPattern)

	if key1 != expectedPattern {
		t.Errorf("Expected pattern '%s', got '%s'", expectedPattern, key1)
	}
}

func TestS3StorageKeyWithDifferentEventTypes(t *testing.T) {
	// Test S3 key generation for different event scenarios
	storage := &S3Storage{
		bucket:   "test-bucket",
		basePath: "multi-event-test",
	}

	tests := []struct {
		name      string
		eventInfo *EventInfo
		filename  string
		expected  string
	}{
		{
			name: "Early year event",
			eventInfo: &EventInfo{
				EventID: "11111",
				Year:    "2020",
				Month:   "Jan",
				Day:     "1",
			},
			filename: "early.bz2",
			expected: "multi-event-test/PRO/2020/Jan/1/11111/early.bz2",
		},
		{
			name: "Mid year event",
			eventInfo: &EventInfo{
				EventID: "22222",
				Year:    "2025",
				Month:   "Jun",
				Day:     "15",
			},
			filename: "mid.bz2",
			expected: "multi-event-test/PRO/2025/Jun/15/22222/mid.bz2",
		},
		{
			name: "End year event",
			eventInfo: &EventInfo{
				EventID: "33333",
				Year:    "2025",
				Month:   "Dec",
				Day:     "31",
			},
			filename: "end.bz2",
			expected: "multi-event-test/PRO/2025/Dec/31/33333/end.bz2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := storage.BuildS3Key(tt.eventInfo, tt.filename)
			// Normalize path separators for cross-platform compatibility
			result = filepath.ToSlash(result)
			expected := filepath.ToSlash(tt.expected)
			if result != expected {
				t.Errorf("Expected '%s', got '%s'", expected, result)
			}
		})
	}
}

func TestS3StorageStructFields(t *testing.T) {
	// Test that S3Storage struct is properly initialized
	tests := []struct {
		name     string
		bucket   string
		basePath string
	}{
		{
			name:     "Standard configuration",
			bucket:   "my-betfair-bucket",
			basePath: "greyhound-data",
		},
		{
			name:     "Empty base path",
			bucket:   "another-bucket",
			basePath: "",
		},
		{
			name:     "Complex bucket name",
			bucket:   "betfair-data-prod-2025",
			basePath: "raw/greyhounds/v2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := &S3Storage{
				bucket:   tt.bucket,
				basePath: tt.basePath,
			}

			if storage.bucket != tt.bucket {
				t.Errorf("Expected bucket '%s', got '%s'", tt.bucket, storage.bucket)
			}

			if storage.basePath != tt.basePath {
				t.Errorf("Expected basePath '%s', got '%s'", tt.basePath, storage.basePath)
			}
		})
	}
}

func TestS3StorageKeyHierarchy(t *testing.T) {
	// Test that the S3 key follows the correct hierarchy: basePath/PRO/Year/Month/Day/EventID/filename
	eventInfo := &EventInfo{
		EventID: "987654321",
		Year:    "2025",
		Month:   "Mar",
		Day:     "10",
	}

	storage := &S3Storage{
		bucket:   "hierarchy-test-bucket",
		basePath: "test/hierarchy",
	}

	result := storage.BuildS3Key(eventInfo, "test-file.bz2")
	// Normalize path separators for cross-platform compatibility
	result = filepath.ToSlash(result)

	expectedParts := []string{"test", "hierarchy", "PRO", "2025", "Mar", "10", "987654321", "test-file.bz2"}
	expectedKey := filepath.ToSlash(filepath.Join(expectedParts...))

	if result != expectedKey {
		t.Errorf("Expected hierarchical key '%s', got '%s'", expectedKey, result)
	}

	// Verify each part of the hierarchy exists in the result
	for _, part := range expectedParts {
		if !contains(result, part) {
			t.Errorf("S3 key should contain hierarchy part '%s'", part)
		}
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
		 len(s) > len(substr)+1 && findSubstring(s, substr))))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}