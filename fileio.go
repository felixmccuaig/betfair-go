package betfair

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dsnet/compress/bzip2"
)

type FileManager struct {
	outputPath string
}

func NewFileManager(outputPath string) *FileManager {
	if outputPath == "" {
		outputPath = "market_files"
	}
	return &FileManager{
		outputPath: outputPath,
	}
}

func (fm *FileManager) CreateMarketWriter(marketID string) (*bufio.Writer, *os.File, error) {
	if err := os.MkdirAll(fm.outputPath, 0755); err != nil {
		return nil, nil, fmt.Errorf("create market_files directory: %w", err)
	}

	filePath := filepath.Join(fm.outputPath, marketID)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, nil, err
	}

	writer := bufio.NewWriter(file)
	return writer, file, nil
}

func (fm *FileManager) GetMarketFilePath(marketID string) string {
	return filepath.Join(fm.outputPath, marketID)
}

func (fm *FileManager) GetCompressedFilePath(marketID string) string {
	return filepath.Join(fm.outputPath, marketID+".bz2")
}

func (fm *FileManager) CompressToBzip2(inputFile, outputFile string) error {
	input, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("open input file: %w", err)
	}
	defer input.Close()

	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer output.Close()

	bz2Writer, err := bzip2.NewWriter(output, &bzip2.WriterConfig{Level: bzip2.DefaultCompression})
	if err != nil {
		return fmt.Errorf("create bzip2 writer: %w", err)
	}
	defer bz2Writer.Close()

	if _, err := io.Copy(bz2Writer, input); err != nil {
		return fmt.Errorf("compress data: %w", err)
	}

	return nil
}

func (fm *FileManager) CleanupFiles(files ...string) {
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			// Silently ignore cleanup errors
		}
	}
}

func BuildEventPath(basePath string, eventInfo *EventInfo) string {
	return filepath.Join(basePath, "PRO", eventInfo.Year, eventInfo.Month, eventInfo.Day, eventInfo.EventID)
}