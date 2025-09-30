package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/felixmccuaig/betfair-go/processor"
)

func main() {
	var (
		s3Path     = flag.String("s3", "", "S3 path to process (e.g., s3://bucket/prefix/)")
		localPath  = flag.String("path", "", "Local file or directory path to process")
		outputPath = flag.String("output", "", "Output CSV file path (e.g., summary_stats.csv)")
		fileLimit  = flag.Int("limit", 0, "Maximum number of files to process (0 = no limit)")
		workers    = flag.Int("workers", 0, "Number of worker goroutines (0 = use CPU count)")
	)
	flag.Parse()

	// Validate input
	if *s3Path == "" && *localPath == "" {
		log.Fatal("Please specify either -s3 or -path")
	}

	if *s3Path != "" && *localPath != "" {
		log.Fatal("Please specify only one of -s3 or -path")
	}

	if *outputPath == "" {
		log.Fatal("Please specify -output with a CSV file path (e.g., summary_stats.csv)")
	}

	// Determine input path
	inputPath := *s3Path
	if inputPath == "" {
		inputPath = *localPath
	}

	log.Printf("Input: %s", inputPath)
	log.Printf("Output: %s", *outputPath)
	if *fileLimit > 0 {
		log.Printf("File limit: %d", *fileLimit)
	}
	if *workers > 0 {
		log.Printf("Workers: %d", *workers)
	}

	// Create market data processor
	mp := processor.NewMarketDataProcessor(*outputPath, *fileLimit, *workers)

	// Process the input path
	if err := mp.ProcessPath(inputPath); err != nil {
		log.Fatalf("Failed to process path: %v", err)
	}

	// Finalize and generate CSV output
	if err := mp.FinalizeProcessing(); err != nil {
		log.Fatalf("Failed to finalize processing: %v", err)
	}

	fmt.Println("Market data processing completed successfully")
}