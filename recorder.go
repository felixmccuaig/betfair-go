package betfair

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type MarketRecorder struct {
	config          *Config
	logger          zerolog.Logger
	streamClient    *StreamClient
	restClient      *RESTClient
	fileManager     *FileManager
	storage         *S3Storage
	marketProcessor *MarketProcessor
	authenticator   *Authenticator
	initialClk      string
	clk             string
	maxRetries      int
	retryDelay      time.Duration
	marketCatalogues map[string]*MarketCatalogue // Cache for market catalogues
}

func NewMarketRecorder(cfg *Config, logger zerolog.Logger) (*MarketRecorder, error) {
	authenticator := NewAuthenticator(cfg.AppKey, os.Getenv("BETFAIR_USERNAME"), os.Getenv("BETFAIR_PASSWORD"))
	streamClient := NewStreamClient(cfg.AppKey, cfg.SessionToken, cfg.HeartbeatMs, logger, authenticator)
	restClient := NewRESTClient(cfg.AppKey, cfg.SessionToken, "en")
	fileManager := NewFileManager(cfg.OutputPath)
	marketProcessor := NewMarketProcessor()

	var storage *S3Storage
	if cfg.S3Bucket != "" {
		var err error
		storage, err = NewS3Storage(context.Background(), cfg.S3Bucket, cfg.S3BasePath)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize S3 storage: %w", err)
		}
	}

	return &MarketRecorder{
		config:           cfg,
		logger:           logger,
		streamClient:     streamClient,
		restClient:       restClient,
		fileManager:      fileManager,
		storage:          storage,
		marketProcessor:  marketProcessor,
		authenticator:    authenticator,
		maxRetries:       5,
		retryDelay:       30 * time.Second,
		marketCatalogues: make(map[string]*MarketCatalogue),
	}, nil
}

func (r *MarketRecorder) Run(ctx context.Context) error {
	writers, files, closeFn, err := r.openWriters()
	if err != nil {
		return err
	}
	defer closeFn()

	marketStatuses := make(map[string]string)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := r.runWithReconnect(ctx, writers, files, marketStatuses); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				r.logger.Error().Err(err).Msg("stream error, will retry")

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(r.retryDelay):
					continue
				}
			}
		}
	}
}

func (r *MarketRecorder) runWithReconnect(ctx context.Context, writers map[string]*bufio.Writer, files map[string]*os.File, marketStatuses map[string]string) error {
	var lastErr error

	for attempt := 1; attempt <= r.maxRetries; attempt++ {
		r.logger.Info().Int("attempt", attempt).Msg("establishing connection")

		stream, err := r.establishConnection(ctx)
		if err != nil {
			lastErr = err
			r.logger.Error().Err(err).Int("attempt", attempt).Msg("failed to establish connection")
			if attempt < r.maxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(r.retryDelay):
					continue
				}
			}
			continue
		}
		defer stream.Close()

		r.logger.Info().Msg("connection established, starting stream processing")

		err = r.processStream(ctx, stream, writers, files, marketStatuses)
		if err != nil {
			lastErr = err
			if r.isRetriableError(err) && attempt < r.maxRetries {
				r.logger.Warn().Err(err).Int("attempt", attempt).Msg("retriable error, will retry")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(r.retryDelay):
					continue
				}
			}
		}
		return err
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

func (r *MarketRecorder) establishConnection(ctx context.Context) (*StreamConn, error) {
	stream, err := r.streamClient.Dial()
	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}

	if err := r.streamClient.Authenticate(stream); err != nil {
		stream.Close()
		if strings.Contains(err.Error(), "session refreshed") {
			r.config.SessionToken = r.streamClient.sessionToken
			r.restClient.UpdateSessionKey(r.streamClient.sessionToken)
		}
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	if err := r.streamClient.RequestHeartbeat(stream); err != nil {
		stream.Close()
		return nil, fmt.Errorf("heartbeat request failed: %w", err)
	}

	marketFilter := r.config.GetMarketFilter()
	if err := r.streamClient.Subscribe(stream, marketFilter, r.initialClk, r.clk); err != nil {
		stream.Close()
		return nil, fmt.Errorf("subscription failed: %w", err)
	}

	r.logger.Info().Msg("subscription established; recording stream")
	return stream, nil
}

func (r *MarketRecorder) processStream(ctx context.Context, stream *StreamConn, writers map[string]*bufio.Writer, files map[string]*os.File, marketStatuses map[string]string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := r.readMessage(ctx, stream, writers, files, marketStatuses); err != nil {
				return err
			}
		}
	}
}

func (r *MarketRecorder) readMessage(ctx context.Context, stream *StreamConn, writers map[string]*bufio.Writer, files map[string]*os.File, marketStatuses map[string]string) error {
	payload, err := stream.ReadMessage()
	if err != nil {
		return err
	}

	initialClk, clk := ExtractAndStoreClock(payload)
	if initialClk != "" {
		r.initialClk = initialClk
	}
	if clk != "" {
		r.clk = clk
	}

	op := ExtractOp(payload)
	if op == "mcm" {
		changeType := ExtractChangeType(payload)
		if changeType == "HEARTBEAT" {
			return nil
		}

		// Parse the message to extract ALL market IDs
		var data map[string]interface{}
		if err := json.Unmarshal(payload, &data); err != nil {
			return fmt.Errorf("failed to parse MCM message: %w", err)
		}

		mc, ok := data["mc"].([]interface{})
		if !ok || len(mc) == 0 {
			return nil
		}

		// Process each market separately
		for _, marketChangeRaw := range mc {
			marketChange, ok := marketChangeRaw.(map[string]interface{})
			if !ok {
				continue
			}

			marketID, ok := marketChange["id"].(string)
			if !ok || marketID == "" {
				continue
			}

			// Fetch market catalogue if we don't have it yet
			if err := r.fetchMarketCatalogue(ctx, marketID); err != nil {
				r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to fetch market catalogue")
				// Continue processing even if catalogue fetch fails
			}

			// Extract status for this specific market
			newStatus := ""
			if marketDef, ok := marketChange["marketDefinition"].(map[string]interface{}); ok {
				if status, ok := marketDef["status"].(string); ok {
					newStatus = status
				}
			}

			var oldStatus string
			marketJustSettled := false
			if newStatus != "" {
				oldStatus = marketStatuses[marketID]
				marketStatuses[marketID] = newStatus
				marketJustSettled = !IsMarketSettled(oldStatus) && IsMarketSettled(newStatus)
			}

			if _, exists := writers[marketID]; !exists {
				if err := r.createWriterForMarket(marketID, writers, files); err != nil {
					r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to create writer for new market")
				} else {
					r.logger.Info().Str("market_id", marketID).Msg("created writer for new market")
				}
			}

			if writer, exists := writers[marketID]; exists {
				// Create a single-market message for this market only
				singleMarketData := map[string]interface{}{
					"op":  data["op"],
					"pt":  data["pt"],
					"clk": data["clk"],
					"mc":  []interface{}{marketChange},
				}

				singleMarketPayload, err := json.Marshal(singleMarketData)
				if err != nil {
					r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to marshal single market message")
					continue
				}

				// Remove the ID field
				filteredPayload, err := RemoveIDField(singleMarketPayload)
				if err != nil {
					r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to filter payload")
					continue
				}

				// Enrich with market catalogue data
				enrichedPayload, err := r.enrichMarketData(marketID, filteredPayload)
				if err != nil {
					r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to enrich market data")
					// Use original filtered payload if enrichment fails
					enrichedPayload = filteredPayload
				}

				if _, err := writer.Write(append(enrichedPayload, '\n')); err != nil {
					r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to write to file")
					continue
				}

				if err := writer.Flush(); err != nil {
					r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to flush file")
					continue
				}
			}

			if marketJustSettled {
				r.logger.Info().Str("market_id", marketID).Str("status", newStatus).Msg("market settled")

				// Create single-market payload for settlement
				singleMarketData := map[string]interface{}{
					"op":  data["op"],
					"pt":  data["pt"],
					"clk": data["clk"],
					"mc":  []interface{}{marketChange},
				}
				singleMarketPayload, _ := json.Marshal(singleMarketData)

				if err := r.handleMarketSettlement(ctx, marketID, singleMarketPayload, writers); err != nil {
					r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to handle market settlement")
				}

				// Clean up market catalogue cache for settled market
				delete(r.marketCatalogues, marketID)
				r.logger.Debug().Str("market_id", marketID).Msg("removed market catalogue from cache")
			}
		}
	}

	return nil
}

func (r *MarketRecorder) handleMarketSettlement(ctx context.Context, marketID string, payload []byte, writers map[string]*bufio.Writer) error {
	if writer, exists := writers[marketID]; exists {
		if err := writer.Flush(); err != nil {
			r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to flush writer")
		}
		delete(writers, marketID)
	}

	eventInfo, err := ExtractEventInfo(payload)
	if err != nil {
		r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to extract event info")
		return nil
	}

	inputFile := r.fileManager.GetMarketFilePath(marketID)
	compressedFile := r.fileManager.GetCompressedFilePath(marketID)

	if err := r.fileManager.CompressToBzip2(inputFile, compressedFile); err != nil {
		r.logger.Error().Err(err).Str("market_id", marketID).Msg("failed to compress file")
		return nil
	}

	r.logger.Info().Str("market_id", marketID).Str("file", compressedFile).Msg("compressed market file")

	if r.storage != nil {
		s3Key := r.storage.BuildS3Key(eventInfo, marketID+".bz2")
		if err := r.storage.Upload(ctx, compressedFile, s3Key); err != nil {
			r.logger.Error().Err(err).Str("market_id", marketID).Str("s3_key", s3Key).Msg("failed to upload to S3")
			return nil
		}

		r.logger.Info().Str("market_id", marketID).Str("s3_key", s3Key).Msg("uploaded market file to S3")
		r.fileManager.CleanupFiles(inputFile, compressedFile)
	}

	return nil
}

func (r *MarketRecorder) openWriters() (map[string]*bufio.Writer, map[string]*os.File, func(), error) {
	writers := make(map[string]*bufio.Writer)
	files := make(map[string]*os.File)

	closer := func() {
		for _, writer := range writers {
			_ = writer.Flush()
		}
		for _, file := range files {
			_ = file.Close()
		}
	}

	if len(r.config.MarketIDs) > 0 {
		for _, marketID := range r.config.MarketIDs {
			if err := r.createWriterForMarket(marketID, writers, files); err != nil {
				closer()
				return nil, nil, nil, fmt.Errorf("open output file for market %s: %w", marketID, err)
			}
		}
	}

	return writers, files, closer, nil
}

func (r *MarketRecorder) createWriterForMarket(marketID string, writers map[string]*bufio.Writer, files map[string]*os.File) error {
	writer, file, err := r.fileManager.CreateMarketWriter(marketID)
	if err != nil {
		return err
	}

	writers[marketID] = writer
	files[marketID] = file
	return nil
}

func (r *MarketRecorder) isRetriableError(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	errStr := err.Error()
	retriableErrors := []string{
		"authentication failed",
		"connection closed",
		"subscription failed",
		"network error",
		"timeout",
		"session refreshed, retry connection",
	}
	for _, retriable := range retriableErrors {
		if strings.Contains(strings.ToLower(errStr), retriable) {
			return true
		}
	}
	return true
}

func (r *MarketRecorder) fetchMarketCatalogue(ctx context.Context, marketID string) error {
	// Check if we already have this market catalogue cached
	if _, exists := r.marketCatalogues[marketID]; exists {
		return nil
	}

	r.logger.Info().Str("market_id", marketID).Msg("fetching market catalogue")

	filter := CreateMarketFilter().WithMarketIDs([]string{marketID})
	projection := []MarketProjection{
		MarketProjectionEvent,
		MarketProjectionMarketDescription,
		MarketProjectionRunnerDescription,
		MarketProjectionEventType,
		MarketProjectionCompetition,
	}

	catalogues, err := r.restClient.ListMarketCatalogue(
		ctx,
		*filter,
		projection,
		MarketSortFirstToStart,
		1,
	)
	if err != nil {
		return fmt.Errorf("failed to fetch market catalogue for %s: %w", marketID, err)
	}

	if len(catalogues) == 0 {
		return fmt.Errorf("no market catalogue found for market %s", marketID)
	}

	// Cache the market catalogue
	r.marketCatalogues[marketID] = &catalogues[0]
	r.logger.Info().Str("market_id", marketID).Str("market_name", catalogues[0].MarketName).Msg("cached market catalogue")

	return nil
}

func (r *MarketRecorder) enrichMarketData(marketID string, payload []byte) ([]byte, error) {
	// Check if we have market catalogue data for this market
	catalogue, exists := r.marketCatalogues[marketID]
	if !exists {
		// Return original payload if no catalogue data available
		return payload, nil
	}

	// Parse the original payload
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	// Navigate to market definition
	mc, ok := data["mc"].([]interface{})
	if !ok || len(mc) == 0 {
		return payload, nil
	}

	market, ok := mc[0].(map[string]interface{})
	if !ok {
		return payload, nil
	}

	marketDef, ok := market["marketDefinition"].(map[string]interface{})
	if !ok {
		return payload, nil
	}

	// Add market name and event information
	marketDef["marketName"] = catalogue.MarketName
	if catalogue.Event != nil {
		marketDef["eventName"] = catalogue.Event.Name
		if catalogue.Event.Venue != "" {
			marketDef["venue"] = catalogue.Event.Venue
		}
	}
	if catalogue.EventType != nil {
		marketDef["eventTypeName"] = catalogue.EventType.Name
	}
	if catalogue.Competition != nil {
		marketDef["competitionName"] = catalogue.Competition.Name
	}

	// Enrich runner information
	runners, ok := marketDef["runners"].([]interface{})
	if ok && len(runners) > 0 {
		// Create a map of runner catalogue data for quick lookup
		runnerMap := make(map[int64]RunnerCatalog)
		for _, catalogueRunner := range catalogue.Runners {
			runnerMap[catalogueRunner.SelectionID] = catalogueRunner
		}

		// Add runner names to each runner in the stream data
		for i, runnerInterface := range runners {
			runner, ok := runnerInterface.(map[string]interface{})
			if !ok {
				continue
			}

			// Get runner ID
			var runnerID int64
			if id, ok := runner["id"].(float64); ok {
				runnerID = int64(id)
			} else {
				continue
			}

			// Add runner name if we have catalogue data
			if catalogueRunner, exists := runnerMap[runnerID]; exists {
				// Add adjustmentFactor first (default 0.0 if not present)
				if _, hasAdjustment := runner["adjustmentFactor"]; !hasAdjustment {
					runner["adjustmentFactor"] = 0.0
				}

				// Use "name" field to match Betfair's format
				runner["name"] = catalogueRunner.RunnerName

				if catalogueRunner.Handicap != 0 {
					runner["handicap"] = catalogueRunner.Handicap
				}
				runner["sortPriority"] = catalogueRunner.SortPriority
			}

			runners[i] = runner
		}
		marketDef["runners"] = runners
	}

	// Marshal back to JSON
	enrichedPayload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal enriched payload: %w", err)
	}

	return enrichedPayload, nil
}