package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/felixmccuaig/betfair-go/processor"
)

func main() {
	var (
		s3Path       = flag.String("s3", "", "S3 path to process (e.g., s3://bucket/prefix/)")
		localPath    = flag.String("path", "", "Local file or directory path to process")
		outputPath   = flag.String("output", "", "Output file path. Can use {date} placeholder (e.g., s3://bucket/summary-{date}.csv)")
		outputFormat = flag.String("format", "csv", "Output format: csv or parquet")
		dateFormat   = flag.String("date-format", "2006-01-02", "Date format for filename (Go time format)")
		fileLimit    = flag.Int("limit", 0, "Maximum number of files to process (0 = no limit)")
		workers      = flag.Int("workers", 0, "Number of worker goroutines (0 = use CPU count)")
		autoDate     = flag.Bool("auto-date", false, "Automatically extract date from input path for output filename")
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
		log.Fatal("Please specify -output")
	}

	// Validate output format
	var format processor.OutputFormat
	switch *outputFormat {
	case "csv":
		format = processor.OutputFormatCSV
	case "parquet":
		format = processor.OutputFormatParquet
	default:
		log.Fatalf("Invalid output format: %s (must be 'csv' or 'parquet')", *outputFormat)
	}

	// Determine input path
	inputPath := *s3Path
	if inputPath == "" {
		inputPath = *localPath
	}

	// Create processor config
	config := processor.ProcessorConfig{
		OutputPath:   *outputPath,
		OutputFormat: format,
		FileLimit:    *fileLimit,
		Workers:      *workers,
		DateFormat:   *dateFormat,
	}

	// Create market data processor
	mp := processor.NewMarketDataProcessorWithConfig(config)

	// If auto-date is enabled, generate the output path
	finalOutputPath := *outputPath
	if *autoDate {
		generatedPath, err := mp.GenerateOutputPath(inputPath)
		if err != nil {
			log.Printf("Warning: could not auto-generate date-based path: %v", err)
			log.Printf("Using provided output path: %s", *outputPath)
		} else {
			finalOutputPath = generatedPath
			mp.OutputFile = generatedPath
			log.Printf("Auto-generated output path: %s", generatedPath)
		}
	}

	log.Printf("Input: %s", inputPath)
	log.Printf("Output: %s", finalOutputPath)
	log.Printf("Format: %s", format)
	if *fileLimit > 0 {
		log.Printf("File limit: %d", *fileLimit)
	}
	if *workers > 0 {
		log.Printf("Workers: %d", *workers)
	}

	// Process the input path
	if err := mp.ProcessPath(inputPath); err != nil {
		log.Fatalf("Failed to process path: %v", err)
	}

	// Finalize and generate output
	if err := mp.FinalizeProcessing(); err != nil {
		log.Fatalf("Failed to finalize processing: %v", err)
	}

	fmt.Println("Market data processing completed successfully")
	os.Exit(0)
}