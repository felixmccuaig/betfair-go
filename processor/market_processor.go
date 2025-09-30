package processor

import (
	"archive/tar"
	"bufio"
	"compress/bzip2"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type MCMMessage struct {
	Op string `json:"op"`
	Pt int64  `json:"pt"`
	Mc []struct {
		ID               string `json:"id"`
		MarketDefinition *struct {
			EventTypeID  string    `json:"eventTypeId"`
			MarketType   string    `json:"marketType"`
			BettingType  string    `json:"bettingType"`
			EventName    string    `json:"eventName"`
			MarketTime   time.Time `json:"marketTime"`
			Runners      []struct {
				ID   int64   `json:"id"`
				Name string  `json:"name"`
				BSP  float64 `json:"bsp"`
			} `json:"runners"`
		} `json:"marketDefinition"`
		RC []struct {
			ID   int64                  `json:"id"`
			LTP  float64                `json:"ltp"`
			TV   float64                `json:"tv"`
			BATB [][]float64            `json:"batb"`
			ATB  [][]float64            `json:"atb"`
			SPB  [][]float64            `json:"spb"`
			TRD  [][]float64            `json:"trd"`
			Raw  map[string]interface{} `json:"-"`
		} `json:"rc"`
	} `json:"mc"`
}

type RunnerState struct {
	Name              string
	BSP               float64
	Updates           []RunnerUpdate
	MaxTV             float64
	LatestLTP         float64
	MaxTradedPrice    float64
	MinTradedPrice    float64
	HasMaxTraded      bool
	HasMinTraded      bool
	Status            string
}

type RunnerUpdate struct {
	Timestamp int64
	LTP       float64
	TV        float64
	BATB      [][]float64
	ATB       [][]float64
	SPB       [][]float64
	TRD       [][]float64
	HasLTP    bool
}

type MarketState struct {
	MarketTime  time.Time
	Venue       string
	EventID     string
	EventName   string
	MarketDef   interface{}
	Runners     map[int64]*RunnerState
}

type SummaryRow struct {
	MarketID              string
	SelectionID           int64
	EventID               string
	EventName             string
	Venue                 string
	GreyhoundName         string
	MarketTime            time.Time
	BSP                   float64
	LTP                   float64
	Price30sBeforeStart   float64
	TotalTradedVolume     float64
	MaxTradedPrice        float64
	MinTradedPrice        float64
	Year                  int
	Month                 int
	Day                   int
	Win                   bool
	HasBSP                bool
	HasLTP                bool
	HasPrice30sBefore     bool
	HasMaxTradedPrice     bool
	HasMinTradedPrice     bool
}

type MarketDataProcessor struct {
	OutputDir       string
	OutputFile      string
	FileLimit       int
	FilesProcessed  int
	MarketStates    map[string]*MarketState
	ProcessedData   []SummaryRow
	VenueRegex      *regexp.Regexp
	GreyhoundRegex  *regexp.Regexp
	Workers         int
	S3Client        *s3.Client
	CurrentSource   string // Track current source file being processed
	mu              sync.RWMutex
}

func NewMarketDataProcessor(outputPath string, fileLimit int, workers int) *MarketDataProcessor {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Determine if outputPath is a file or directory
	var outputDir, outputFile string
	if outputPath != "" {
		if strings.HasSuffix(outputPath, ".csv") {
			outputFile = outputPath
			outputDir = filepath.Dir(outputPath)
		} else {
			outputDir = outputPath
		}
		os.MkdirAll(outputDir, 0755)
	} else {
		outputDir = "processed_market_data"
		os.MkdirAll(outputDir, 0755)
	}

	// Initialize S3 client
	cfg, err := config.LoadDefaultConfig(context.Background())
	var s3Client *s3.Client
	if err == nil {
		s3Client = s3.NewFromConfig(cfg)
	} else {
		log.Printf("Warning: failed to load AWS config: %v", err)
	}

	venueRegex := regexp.MustCompile(`\s*\([A-Z]{2,3}\)\s*\d+\w*\s*\w+`)
	greyhoundRegex := regexp.MustCompile(`^\d+\.\s*`)

	return &MarketDataProcessor{
		OutputDir:      outputDir,
		OutputFile:     outputFile,
		FileLimit:      fileLimit,
		Workers:        workers,
		MarketStates:   make(map[string]*MarketState),
		VenueRegex:     venueRegex,
		GreyhoundRegex: greyhoundRegex,
		S3Client:       s3Client,
	}
}

func (p *MarketDataProcessor) extractVenueFromEventName(eventName string) string {
	clean := strings.TrimSpace(eventName)
	if clean == "" {
		return ""
	}

	if idx := strings.Index(clean, "("); idx > 0 {
		clean = strings.TrimSpace(clean[:idx])
	}

	if p.VenueRegex != nil {
		stripped := strings.TrimSpace(p.VenueRegex.ReplaceAllString(clean, ""))
		if stripped != "" {
			clean = stripped
		}
	}

	return clean
}

func (p *MarketDataProcessor) extractGreyhoundName(runnerName string) string {
	name := p.GreyhoundRegex.ReplaceAllString(runnerName, "")
	return strings.TrimSpace(name)
}

func (p *MarketDataProcessor) isGreyhoundWinMarket(marketDef map[string]interface{}) bool {
	eventTypeID, ok := marketDef["eventTypeId"].(string)
	if !ok || eventTypeID != "4339" {
		return false
	}

	marketType, ok := marketDef["marketType"].(string)
	if !ok || marketType != "WIN" {
		return false
	}

	bettingType, ok := marketDef["bettingType"].(string)
	if !ok || bettingType != "ODDS" {
		return false
	}

	return true
}

func (p *MarketDataProcessor) getPrice30sBeforeStart(updates []RunnerUpdate, marketTime time.Time) (float64, bool) {
	targetTimestamp := marketTime.Add(-30 * time.Second).UnixMilli()

	var bestBefore struct {
		price    float64
		timeDiff int64
		hasPrice bool
	}
	bestBefore.timeDiff = int64(^uint64(0) >> 1) // max int64

	var bestAfter struct {
		price    float64
		timeDiff int64
		hasPrice bool
	}
	bestAfter.timeDiff = int64(^uint64(0) >> 1) // max int64

	for _, update := range updates {
		var price float64
		var hasPrice bool

		if update.HasLTP {
			price = update.LTP
			hasPrice = true
		} else if len(update.BATB) > 0 && len(update.BATB[0]) > 0 {
			price = update.BATB[0][0]
			hasPrice = true
		} else if len(update.ATB) > 0 && len(update.ATB[0]) > 0 {
			price = update.ATB[0][0]
			hasPrice = true
		} else if len(update.SPB) > 0 && len(update.SPB[0]) > 0 {
			price = update.SPB[0][0]
			hasPrice = true
		} else if len(update.TRD) > 0 && len(update.TRD[len(update.TRD)-1]) > 0 {
			price = update.TRD[len(update.TRD)-1][0]
			hasPrice = true
		}

		if !hasPrice {
			continue
		}

		diff := targetTimestamp - update.Timestamp
		if diff >= 0 {
			if diff < bestBefore.timeDiff {
				bestBefore.price = price
				bestBefore.timeDiff = diff
				bestBefore.hasPrice = true
			}
		} else {
			absDiff := -diff
			if absDiff < bestAfter.timeDiff {
				bestAfter.price = price
				bestAfter.timeDiff = absDiff
				bestAfter.hasPrice = true
			}
		}
	}

	if bestBefore.hasPrice {
		return bestBefore.price, true
	}
	if bestAfter.hasPrice {
		return bestAfter.price, true
	}
	return 0, false
}

func (p *MarketDataProcessor) processMCMMessage(mcmData map[string]interface{}) {
	mc, ok := mcmData["mc"].([]interface{})
	if !ok {
		return
	}

	timestamp, _ := mcmData["pt"].(float64)

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, marketChangeRaw := range mc {
		marketChange, ok := marketChangeRaw.(map[string]interface{})
		if !ok {
			continue
		}

		marketID, ok := marketChange["id"].(string)
		if !ok {
			continue
		}

		// Check if this is a new market definition
		if marketDefRaw, exists := marketChange["marketDefinition"]; exists {
			marketDef, ok := marketDefRaw.(map[string]interface{})
			if !ok {
				continue
			}

			// Only process greyhound WIN markets for new markets or full definitions
			_, marketExists := p.MarketStates[marketID]
			hasEventTypeId := marketDef["eventTypeId"] != nil
			if !marketExists && hasEventTypeId && !p.isGreyhoundWinMarket(marketDef) {
				continue
			}

			// Extract market info (for full market definitions)
			var marketTime time.Time
			var venue string
			var eventID string
			var eventName string

			// Extract eventName, eventID, and venue if present
			if en, ok := marketDef["eventName"].(string); ok {
				eventName = en
			}
			if eid, ok := marketDef["eventId"].(string); ok {
				eventID = eid
			}
			// Venue can come from either the venue field or extracted from eventName
			if v, ok := marketDef["venue"].(string); ok {
				venue = v
			} else if eventName != "" {
				venue = p.extractVenueFromEventName(eventName)
			}

			// Extract marketTime if present
			if marketTimeStr, ok := marketDef["marketTime"].(string); ok {
				var err error
				marketTime, err = time.Parse(time.RFC3339, marketTimeStr)
				if err != nil {
					continue
				}
			}

			if _, exists := p.MarketStates[marketID]; !exists {
				// First time seeing this market - only create if we have full market info
				if _, ok := marketDef["marketTime"].(string); ok {
					p.MarketStates[marketID] = &MarketState{
						MarketTime: marketTime,
						Venue:      venue,
						EventID:    eventID,
						EventName:  eventName,
						MarketDef:  marketDef,
						Runners:    make(map[int64]*RunnerState),
					}

					// Debug print when creating market 1.248394060
					if marketID == "1.248394060" {
						log.Printf("DEBUG: CREATED market 1.248394060 in file %s - EventID=%s, EventName=%q, Venue=%q",
							p.CurrentSource, eventID, eventName, venue)
					}
				} else {
					// Skip partial market definition for non-existing markets
					continue
				}

				runnersRaw, ok := marketDef["runners"].([]interface{})
				if ok {
					for _, runnerRaw := range runnersRaw {
						runner, ok := runnerRaw.(map[string]interface{})
						if !ok {
							continue
						}

						runnerIDFloat, ok := runner["id"].(float64)
						if !ok {
							continue
						}
						runnerID := int64(runnerIDFloat)

						runnerName, _ := runner["name"].(string)
						bsp, _ := runner["bsp"].(float64)
						status, _ := runner["status"].(string)

						p.MarketStates[marketID].Runners[runnerID] = &RunnerState{
							Name:    p.extractGreyhoundName(runnerName),
							BSP:     bsp,
							Updates: make([]RunnerUpdate, 0),
							Status:  status,
						}
					}
				}
			} else {
				// Update existing market
				marketState := p.MarketStates[marketID]

				// Only update fields if they have values
				if !marketTime.IsZero() {
					marketState.MarketTime = marketTime
				}
				if venue != "" {
					marketState.Venue = venue
				}
				if eventID != "" {
					marketState.EventID = eventID
				}
				if eventName != "" {
					marketState.EventName = eventName
				}
				marketState.MarketDef = marketDef

				runnersRaw, ok := marketDef["runners"].([]interface{})
				if ok {
					for _, runnerRaw := range runnersRaw {
						runner, ok := runnerRaw.(map[string]interface{})
						if !ok {
							continue
						}

						runnerIDFloat, ok := runner["id"].(float64)
						if !ok {
							continue
						}
						runnerID := int64(runnerIDFloat)

						runnerState, exists := marketState.Runners[runnerID]
						if !exists {
							runnerName, _ := runner["name"].(string)
							bsp, _ := runner["bsp"].(float64)
							status, _ := runner["status"].(string)
							marketState.Runners[runnerID] = &RunnerState{
								Name:    p.extractGreyhoundName(runnerName),
								BSP:     bsp,
								Updates: make([]RunnerUpdate, 0),
								Status:  status,
							}
						} else {
							runnerName, _ := runner["name"].(string)
							if runnerName != "" {
								runnerState.Name = p.extractGreyhoundName(runnerName)
							}

							if bsp, ok := runner["bsp"].(float64); ok {
								runnerState.BSP = bsp
							}

							if status, ok := runner["status"].(string); ok {
								runnerState.Status = status
							}
						}
					}
				}
			}
		}

		// Process runner changes
		if marketState, exists := p.MarketStates[marketID]; exists {
			if rcRaw, exists := marketChange["rc"]; exists {
				rc, ok := rcRaw.([]interface{})
				if !ok {
					continue
				}

				for _, runnerChangeRaw := range rc {
					runnerChange, ok := runnerChangeRaw.(map[string]interface{})
					if !ok {
						continue
					}

					runnerIDFloat, ok := runnerChange["id"].(float64)
					if !ok {
						continue
					}
					runnerID := int64(runnerIDFloat)

					if runnerState, exists := marketState.Runners[runnerID]; exists {
						update := RunnerUpdate{
							Timestamp: int64(timestamp),
						}

						if ltp, ok := runnerChange["ltp"].(float64); ok {
							update.LTP = ltp
							update.HasLTP = true
							runnerState.LatestLTP = ltp
						}

						if tv, ok := runnerChange["tv"].(float64); ok {
							update.TV = tv
							if tv > runnerState.MaxTV {
								runnerState.MaxTV = tv
							}
						}

						// Handle BATB, ATB, SPB, TRD arrays
						if batb, ok := runnerChange["batb"].([]interface{}); ok {
							update.BATB = convertToFloat64Array(batb)
						}

						if atb, ok := runnerChange["atb"].([]interface{}); ok {
							update.ATB = convertToFloat64Array(atb)
						}

						if spb, ok := runnerChange["spb"].([]interface{}); ok {
							update.SPB = convertToFloat64Array(spb)
						}

						if trd, ok := runnerChange["trd"].([]interface{}); ok {
							update.TRD = convertToFloat64Array(trd)

							// Update max/min traded prices
							for _, trade := range update.TRD {
								if len(trade) > 0 {
									price := trade[0]
									if !runnerState.HasMaxTraded || price > runnerState.MaxTradedPrice {
										runnerState.MaxTradedPrice = price
										runnerState.HasMaxTraded = true
									}
									if !runnerState.HasMinTraded || price < runnerState.MinTradedPrice {
										runnerState.MinTradedPrice = price
										runnerState.HasMinTraded = true
									}
								}
							}

							// Calculate total volume from trades if TV not present
							if _, hasTv := runnerChange["tv"]; !hasTv {
								tradedTotal := 0.0
								for _, trade := range update.TRD {
									if len(trade) > 1 {
										tradedTotal += trade[1]
									}
								}
								if tradedTotal > runnerState.MaxTV {
									runnerState.MaxTV = tradedTotal
								}
							}
						}

						runnerState.Updates = append(runnerState.Updates, update)
					}
				}
			}
		}
	}
}

func convertToFloat64Array(arr []interface{}) [][]float64 {
	result := make([][]float64, 0, len(arr))
	for _, item := range arr {
		if subArr, ok := item.([]interface{}); ok {
			subResult := make([]float64, 0, len(subArr))
			for _, subItem := range subArr {
				if val, ok := subItem.(float64); ok {
					subResult = append(subResult, val)
				}
			}
			if len(subResult) > 0 {
				result = append(result, subResult)
			}
		}
	}
	return result
}

func (p *MarketDataProcessor) finalizeMarket(marketID string) []SummaryRow {
	marketState, exists := p.MarketStates[marketID]
	if !exists {
		return nil
	}

	var summaryRows []SummaryRow

	for runnerID, runnerData := range marketState.Runners {
		price30sBefore, hasPrice30sBefore := p.getPrice30sBeforeStart(runnerData.Updates, marketState.MarketTime)

		row := SummaryRow{
			MarketID:              marketID,
			SelectionID:           runnerID,
			EventID:               marketState.EventID,
			EventName:             marketState.EventName,
			Venue:                 marketState.Venue,
			GreyhoundName:         runnerData.Name,
			MarketTime:            marketState.MarketTime,
			BSP:                   runnerData.BSP,
			LTP:                   runnerData.LatestLTP,
			Price30sBeforeStart:   price30sBefore,
			TotalTradedVolume:     runnerData.MaxTV,
			MaxTradedPrice:        runnerData.MaxTradedPrice,
			MinTradedPrice:        runnerData.MinTradedPrice,
			Year:                  marketState.MarketTime.Year(),
			Month:                 int(marketState.MarketTime.Month()),
			Day:                   marketState.MarketTime.Day(),
			Win:                   runnerData.Status == "WINNER",
			HasBSP:                runnerData.BSP != 0,
			HasLTP:                runnerData.LatestLTP != 0,
			HasPrice30sBefore:     hasPrice30sBefore,
			HasMaxTradedPrice:     runnerData.HasMaxTraded,
			HasMinTradedPrice:     runnerData.HasMinTraded,
		}

		// Debug print for specific market
		if marketID == "1.248394060" {
			log.Printf("DEBUG: Market 1.248394060 - EventID=%s, EventName=%s, Venue=%s, Runner=%s",
				marketState.EventID, marketState.EventName, marketState.Venue, runnerData.Name)
		}

		summaryRows = append(summaryRows, row)
	}

	delete(p.MarketStates, marketID)
	return summaryRows
}

func (p *MarketDataProcessor) ProcessFile(filePath string) error {
	// Thread-safe check for file limit
	p.mu.RLock()
	filesProcessed := p.FilesProcessed
	p.mu.RUnlock()

	if p.FileLimit > 0 && filesProcessed >= p.FileLimit {
		log.Printf("File limit reached (%d); skipping %s", p.FileLimit, filePath)
		return nil
	}

	log.Printf("Processing file: %s", filePath)

	// Check if this is an S3 path
	if strings.HasPrefix(filePath, "s3://") {
		return p.processS3File(filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var reader io.Reader = file

	// Handle bz2 compression
	if strings.HasSuffix(filePath, ".bz2") {
		reader = bzip2.NewReader(file)
	}

	return p.processReader(reader, filePath)
}

func (p *MarketDataProcessor) processReader(reader io.Reader, sourceName string) error {
	// Store current source for debug purposes
	p.mu.Lock()
	p.CurrentSource = sourceName
	p.mu.Unlock()

	// Extract expected market ID from filename (if it follows the pattern)
	expectedMarketID := p.extractMarketIDFromPath(sourceName)

	// Track all unique market IDs found in this file
	foundMarketIDs := make(map[string]bool)
	mismatchCount := 0

	scanner := bufio.NewScanner(reader)
	lineCount := 0

	for scanner.Scan() {
		lineCount++
		line := scanner.Text()

		var mcmData map[string]interface{}
		if err := json.Unmarshal([]byte(line), &mcmData); err != nil {
			continue
		}

		if op, ok := mcmData["op"].(string); ok && op == "mcm" {
			// Validate that markets in this file match the expected market ID
			if expectedMarketID != "" {
				if mc, ok := mcmData["mc"].([]interface{}); ok {
					for _, marketChangeRaw := range mc {
						if marketChange, ok := marketChangeRaw.(map[string]interface{}); ok {
							if marketID, ok := marketChange["id"].(string); ok {
								// Track this market ID
								if !foundMarketIDs[marketID] {
									foundMarketIDs[marketID] = true
									// Log first occurrence of each unique market ID
									if marketID != expectedMarketID {
										log.Printf("⚠️  CONTAMINATION: File %s contains market %s (expected %s) at line %d",
											filepath.Base(sourceName), marketID, expectedMarketID, lineCount)
									}
								}

								// Count mismatches
								if marketID != expectedMarketID {
									mismatchCount++
								}
							}
						}
					}
				}
			}

			// Check if this message contains market 1.248394060 (debug)
			if mc, ok := mcmData["mc"].([]interface{}); ok {
				for _, marketChangeRaw := range mc {
					if marketChange, ok := marketChangeRaw.(map[string]interface{}); ok {
						if marketID, ok := marketChange["id"].(string); ok && marketID == "1.248394060" {
							log.Printf("DEBUG: Found market 1.248394060 in source: %s at line %d", sourceName, lineCount)
							if marketDef, ok := marketChange["marketDefinition"].(map[string]interface{}); ok {
								log.Printf("DEBUG: Market definition present: eventId=%v, eventName=%v",
									marketDef["eventId"], marketDef["eventName"])
							}
						}
					}
				}
			}
			p.processMCMMessage(mcmData)
		}

		if lineCount%10000 == 0 {
			log.Printf("Processed %d lines from %s", lineCount, sourceName)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Warning: error reading %s: %v", sourceName, err)
	}

	// Report contamination summary for this file
	if expectedMarketID != "" && len(foundMarketIDs) > 0 {
		if len(foundMarketIDs) == 1 && foundMarketIDs[expectedMarketID] {
			// Clean file - only contains expected market
			log.Printf("✅ File %s is clean (market %s only)", filepath.Base(sourceName), expectedMarketID)
		} else {
			// Contaminated file
			var otherMarkets []string
			for marketID := range foundMarketIDs {
				if marketID != expectedMarketID {
					otherMarkets = append(otherMarkets, marketID)
				}
			}
			log.Printf("❌ File %s is CONTAMINATED: contains %d unique markets, %d mismatch instances. Other markets: %v",
				filepath.Base(sourceName), len(foundMarketIDs), mismatchCount, otherMarkets)
		}
	}

	log.Printf("Completed processing %d lines from %s", lineCount, sourceName)

	// Thread-safe increment of FilesProcessed
	p.mu.Lock()
	p.FilesProcessed++
	p.mu.Unlock()

	return nil
}

// extractMarketIDFromPath extracts the market ID from a file path like "1.248394055.bz2"
func (p *MarketDataProcessor) extractMarketIDFromPath(path string) string {
	// Extract filename from path
	filename := filepath.Base(path)

	// Remove extensions (.bz2, .json, .jsonl, etc)
	filename = strings.TrimSuffix(filename, ".bz2")
	filename = strings.TrimSuffix(filename, ".json")
	filename = strings.TrimSuffix(filename, ".jsonl")

	// Check if it looks like a market ID (starts with "1.")
	if strings.HasPrefix(filename, "1.") {
		return filename
	}

	return ""
}

func (p *MarketDataProcessor) processPath(inputPath string) error {
	// Check if this is an S3 path
	if strings.HasPrefix(inputPath, "s3://") {
		return p.processS3Path(inputPath)
	}

	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("path does not exist: %s", inputPath)
	}

	if info.IsDir() {
		return p.processDirectory(inputPath)
	}

	if p.isSupportedFile(inputPath) {
		return p.ProcessFile(inputPath)
	}

	log.Printf("Warning: skipping unsupported file type: %s", inputPath)
	return nil
}

// ProcessPath is the main entry point for processing any path (local or S3)
func (p *MarketDataProcessor) ProcessPath(inputPath string) error {
	return p.processPath(inputPath)
}

func (p *MarketDataProcessor) processDirectory(dirPath string) error {
	var supportedFiles []string

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && p.isSupportedFile(path) {
			supportedFiles = append(supportedFiles, path)
		}

		return nil
	})

	if err != nil {
		return err
	}

	sort.Strings(supportedFiles)

	if len(supportedFiles) == 0 {
		log.Printf("Warning: no supported files found under %s", dirPath)
		return nil
	}

	return p.processFilesParallel(supportedFiles)
}

func (p *MarketDataProcessor) processFilesParallel(filePaths []string) error {
	// Create a channel for file paths
	filesCh := make(chan string, len(filePaths))
	errorsCh := make(chan error, len(filePaths))

	// Add files to channel, respecting file limit
	filesToProcess := filePaths
	if p.FileLimit > 0 && len(filePaths) > p.FileLimit {
		filesToProcess = filePaths[:p.FileLimit]
	}

	for _, filePath := range filesToProcess {
		filesCh <- filePath
	}
	close(filesCh)

	// Create wait group for workers
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < p.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filePath := range filesCh {
				if err := p.ProcessFile(filePath); err != nil {
					log.Printf("Error processing file %s: %v", filePath, err)
					errorsCh <- err
				} else {
					errorsCh <- nil
				}
			}
		}()
	}

	// Wait for all workers to complete
	wg.Wait()
	close(errorsCh)

	// Check for any errors
	var lastError error
	for err := range errorsCh {
		if err != nil {
			lastError = err
		}
	}

	return lastError
}

func (p *MarketDataProcessor) isSupportedFile(filePath string) bool {
	if strings.HasPrefix(filepath.Base(filePath), ".") {
		return false
	}

	ext := filepath.Ext(filePath)
	return ext == ".bz2" || ext == ".jsonl" || ext == ".json" || ext == ""
}

func (p *MarketDataProcessor) saveMonthlyData(year, month int, data []SummaryRow) error {
	if len(data) == 0 {
		return nil
	}

	filename := fmt.Sprintf("greyhound_win_markets_%d_%02d.csv", year, month)
	outputPath := filepath.Join(p.OutputDir, filename)

	// Check if file exists to determine if we need to write header
	fileExists := false
	if _, err := os.Stat(outputPath); err == nil {
		fileExists = true
	}

	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile(outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header only if file is new
	if !fileExists {
		header := []string{
			"market_id", "selection_id", "event_id", "event_name", "venue", "greyhound_name", "market_time",
			"bsp", "ltp", "price_30s_before_start", "total_traded_volume",
			"max_traded_price", "min_traded_price", "year", "month", "day", "win",
		}
		if err := writer.Write(header); err != nil {
			return err
		}
	}

	// Write data
	for _, row := range data {
		record := []string{
			row.MarketID,
			strconv.FormatInt(row.SelectionID, 10),
			row.EventID,
			row.EventName,
			row.Venue,
			row.GreyhoundName,
			row.MarketTime.Format(time.RFC3339),
			formatFloat(row.BSP, row.HasBSP),
			formatFloat(row.LTP, row.HasLTP),
			formatFloat(row.Price30sBeforeStart, row.HasPrice30sBefore),
			strconv.FormatFloat(row.TotalTradedVolume, 'f', -1, 64),
			formatFloat(row.MaxTradedPrice, row.HasMaxTradedPrice),
			formatFloat(row.MinTradedPrice, row.HasMinTradedPrice),
			strconv.Itoa(row.Year),
			strconv.Itoa(row.Month),
			strconv.Itoa(row.Day),
			strconv.FormatBool(row.Win),
		}

		if err := writer.Write(record); err != nil {
			return err
		}
	}

	if fileExists {
		log.Printf("Appended %d records to %s", len(data), outputPath)
	} else {
		log.Printf("Created %s with %d records", outputPath, len(data))
	}
	return nil
}

func formatFloat(value float64, hasValue bool) string {
	if !hasValue || value == 0 {
		return ""
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func (p *MarketDataProcessor) FinalizeProcessing() error {
	log.Println("Finalizing processing...")

	// Collect all data
	var allData []SummaryRow

	// Finalize any remaining markets
	for marketID := range p.MarketStates {
		summaryRows := p.finalizeMarket(marketID)
		if summaryRows != nil {
			allData = append(allData, summaryRows...)
		}
	}

	// Add previously processed data
	allData = append(allData, p.ProcessedData...)

	if len(allData) == 0 {
		log.Println("No data to save")
		return nil
	}

	// If single output file is specified, write all data to one file
	if p.OutputFile != "" {
		return p.saveSingleCSV(p.OutputFile, allData)
	}

	// Otherwise, group by month and save monthly files
	monthlyData := make(map[string][]SummaryRow)
	for _, row := range allData {
		key := fmt.Sprintf("%d_%02d", row.Year, row.Month)
		monthlyData[key] = append(monthlyData[key], row)
	}

	// Save monthly files
	for _, data := range monthlyData {
		if len(data) > 0 {
			year := data[0].Year
			month := data[0].Month
			if err := p.saveMonthlyData(year, month, data); err != nil {
				return err
			}
		}
	}

	log.Printf("Processing complete. Generated %d monthly files.", len(monthlyData))
	return nil
}

func (p *MarketDataProcessor) saveSingleCSV(outputPath string, data []SummaryRow) error {
	if len(data) == 0 {
		return nil
	}

	// Ensure directory exists
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"market_id", "selection_id", "event_id", "event_name", "venue", "greyhound_name", "market_time",
		"bsp", "ltp", "price_30s_before_start", "total_traded_volume",
		"max_traded_price", "min_traded_price", "year", "month", "day", "win",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write data
	for _, row := range data {
		record := []string{
			row.MarketID,
			strconv.FormatInt(row.SelectionID, 10),
			row.EventID,
			row.EventName,
			row.Venue,
			row.GreyhoundName,
			row.MarketTime.Format(time.RFC3339),
			formatFloat(row.BSP, row.HasBSP),
			formatFloat(row.LTP, row.HasLTP),
			formatFloat(row.Price30sBeforeStart, row.HasPrice30sBefore),
			strconv.FormatFloat(row.TotalTradedVolume, 'f', -1, 64),
			formatFloat(row.MaxTradedPrice, row.HasMaxTradedPrice),
			formatFloat(row.MinTradedPrice, row.HasMinTradedPrice),
			strconv.Itoa(row.Year),
			strconv.Itoa(row.Month),
			strconv.Itoa(row.Day),
			strconv.FormatBool(row.Win),
		}

		if err := writer.Write(record); err != nil {
			return err
		}
	}

	log.Printf("Created %s with %d records", outputPath, len(data))
	return nil
}

// ProcessTarFile processes a tar archive by streaming through it and processing each .bz2 file
func ProcessTarFile(reader io.Reader, progressCallback func(filename string, records []SummaryRow)) error {
	tarReader := tar.NewReader(reader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if header.Typeflag != tar.TypeReg {
			continue
		}

		// Only process .bz2 files
		if !strings.HasSuffix(header.Name, ".bz2") {
			continue
		}

		// Create a new processor for each file to avoid memory issues
		processor := NewMarketDataProcessor("", 0, 1)

		// Process the file directly from the tar stream
		err = processor.ProcessFile(header.Name)
		if err != nil {
			log.Printf("Warning: failed to process %s: %v", header.Name, err)
			continue
		}

		// Finalize and get records
		records := processor.ProcessedData
		if err != nil {
			log.Printf("Warning: failed to process %s: %v", header.Name, err)
			continue
		}

		// Call progress callback if provided
		if progressCallback != nil {
			progressCallback(header.Name, records)
		}
	}

	return nil
}

// parseS3Path parses an S3 path into bucket and key
func parseS3Path(s3Path string) (bucket, key string, err error) {
	if !strings.HasPrefix(s3Path, "s3://") {
		return "", "", fmt.Errorf("invalid S3 path: %s", s3Path)
	}

	path := strings.TrimPrefix(s3Path, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 1 {
		return "", "", fmt.Errorf("invalid S3 path format: %s", s3Path)
	}

	bucket = parts[0]
	if len(parts) > 1 {
		key = parts[1]
	}

	return bucket, key, nil
}

// processS3File processes a single S3 file
func (p *MarketDataProcessor) processS3File(s3Path string) error {
	if p.S3Client == nil {
		return fmt.Errorf("S3 client not initialized")
	}

	bucket, key, err := parseS3Path(s3Path)
	if err != nil {
		return err
	}

	ctx := context.Background()
	result, err := p.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to get S3 object %s: %w", s3Path, err)
	}
	defer result.Body.Close()

	var reader io.Reader = result.Body

	// Handle bz2 compression
	if strings.HasSuffix(key, ".bz2") {
		reader = bzip2.NewReader(result.Body)
	}

	return p.processReader(reader, s3Path)
}

// processS3Path processes an S3 path (can be a file or a "directory" prefix)
func (p *MarketDataProcessor) processS3Path(s3Path string) error {
	if p.S3Client == nil {
		return fmt.Errorf("S3 client not initialized")
	}

	bucket, prefix, err := parseS3Path(s3Path)
	if err != nil {
		return err
	}

	// Add trailing slash to prefix if not empty and doesn't have one
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	// List objects with the prefix
	ctx := context.Background()
	var supportedFiles []string

	paginator := s3.NewListObjectsV2Paginator(p.S3Client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			key := *obj.Key
			// Skip directories
			if strings.HasSuffix(key, "/") {
				continue
			}

			// Check if supported file type
			if p.isSupportedFile(key) {
				fullPath := fmt.Sprintf("s3://%s/%s", bucket, key)
				supportedFiles = append(supportedFiles, fullPath)
			}
		}
	}

	if len(supportedFiles) == 0 {
		log.Printf("Warning: no supported files found in %s", s3Path)
		return nil
	}

	log.Printf("Found %d files to process in %s", len(supportedFiles), s3Path)
	return p.processFilesParallel(supportedFiles)
}
