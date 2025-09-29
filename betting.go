package betfair

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Betting Types
type Side string

const (
	SideBack Side = "BACK"
	SideLay  Side = "LAY"
)

type OrderType string

const (
	OrderTypeLimit            OrderType = "LIMIT"
	OrderTypeLimitOnClose     OrderType = "LIMIT_ON_CLOSE"
	OrderTypeMarketOnClose    OrderType = "MARKET_ON_CLOSE"
)

type PersistenceType string

const (
	PersistenceLapse  PersistenceType = "LAPSE"
	PersistencePersist PersistenceType = "PERSIST"
	PersistenceMarketOnClose PersistenceType = "MARKET_ON_CLOSE"
)

type BetStatus string

const (
	BetStatusSettled   BetStatus = "SETTLED"
	BetStatusVoided    BetStatus = "VOIDED"
	BetStatusLapsed    BetStatus = "LAPSED"
	BetStatusCancelled BetStatus = "CANCELLED"
)

type OrderProjection string

const (
	OrderProjectionAll         OrderProjection = "ALL"
	OrderProjectionExecutable  OrderProjection = "EXECUTABLE"
	OrderProjectionExecutionComplete OrderProjection = "EXECUTION_COMPLETE"
)

type OrderBy string

const (
	OrderByBY      OrderBy = "BY_BET"
	OrderByMarket  OrderBy = "BY_MARKET"
	OrderBySettledTime OrderBy = "BY_SETTLED_TIME"
)

type SortDir string

const (
	SortDirEarliestToLatest SortDir = "EARLIEST_TO_LATEST"
	SortDirLatestToEarliest SortDir = "LATEST_TO_EARLIEST"
)

type GroupBy string

const (
	GroupByEventType GroupBy = "EVENT_TYPE"
	GroupByEvent     GroupBy = "EVENT"
	GroupByMarket    GroupBy = "MARKET"
	GroupBySide      GroupBy = "SIDE"
	GroupByBet       GroupBy = "BET"
)

// Betting Instruction Types
type PlaceInstruction struct {
	OrderType         OrderType       `json:"orderType"`
	SelectionID       int64           `json:"selectionId"`
	Handicap          *float64        `json:"handicap,omitempty"`
	Side              Side            `json:"side"`
	LimitOrder        *LimitOrder     `json:"limitOrder,omitempty"`
	LimitOnCloseOrder *LimitOnCloseOrder `json:"limitOnCloseOrder,omitempty"`
	MarketOnCloseOrder *MarketOnCloseOrder `json:"marketOnCloseOrder,omitempty"`
	CustomerOrderRef  string          `json:"customerOrderRef,omitempty"`
}

type LimitOrder struct {
	Size            float64         `json:"size"`
	Price           float64         `json:"price"`
	PersistenceType PersistenceType `json:"persistenceType"`
	TimeInForce     *string         `json:"timeInForce,omitempty"`
	MinFillSize     *float64        `json:"minFillSize,omitempty"`
	BetTargetType   *string         `json:"betTargetType,omitempty"`
	BetTargetSize   *float64        `json:"betTargetSize,omitempty"`
}

type LimitOnCloseOrder struct {
	Size  float64 `json:"size"`
	Price float64 `json:"price"`
}

type MarketOnCloseOrder struct {
	Size float64 `json:"size"`
}

type CancelInstruction struct {
	BetID         string   `json:"betId"`
	SizeReduction *float64 `json:"sizeReduction,omitempty"`
}

type ReplaceInstruction struct {
	BetID    string  `json:"betId"`
	NewPrice float64 `json:"newPrice"`
}

type UpdateInstruction struct {
	BetID               string          `json:"betId"`
	NewPersistenceType  PersistenceType `json:"newPersistenceType"`
}

// Market Book Types
type PriceProjection struct {
	PriceData                []PriceData `json:"priceData,omitempty"`
	ExBestOffersOverrides    *ExBestOffersOverrides `json:"exBestOffersOverrides,omitempty"`
	Virtualise               *bool       `json:"virtualise,omitempty"`
	RolloverStakes           *bool       `json:"rolloverStakes,omitempty"`
}

type PriceData string

const (
	PriceDataSPAvailable PriceData = "SP_AVAILABLE"
	PriceDataSPTraded    PriceData = "SP_TRADED"
	PriceDataEXBestOffers PriceData = "EX_BEST_OFFERS"
	PriceDataEXAllOffers  PriceData = "EX_ALL_OFFERS"
	PriceDataEXTraded     PriceData = "EX_TRADED"
)

type ExBestOffersOverrides struct {
	BestPricesDepth          *int    `json:"bestPricesDepth,omitempty"`
	RollupModel              *string `json:"rollupModel,omitempty"`
	RollupLimit              *int    `json:"rollupLimit,omitempty"`
	RollupLiabilityThreshold *float64 `json:"rollupLiabilityThreshold,omitempty"`
	RollupLiabilityFactor    *int    `json:"rollupLiabilityFactor,omitempty"`
}

type MarketBook struct {
	MarketID              string       `json:"marketId"`
	IsMarketDataDelayed   bool         `json:"isMarketDataDelayed"`
	Status                string       `json:"status"`
	BetDelay              int          `json:"betDelay"`
	BspReconciled         bool         `json:"bspReconciled"`
	Complete              bool         `json:"complete"`
	InPlay                bool         `json:"inplay"`
	NumberOfWinners       int          `json:"numberOfWinners"`
	NumberOfRunners       int          `json:"numberOfRunners"`
	NumberOfActiveRunners int          `json:"numberOfActiveRunners"`
	LastMatchTime         *time.Time   `json:"lastMatchTime,omitempty"`
	TotalMatched          float64      `json:"totalMatched"`
	TotalAvailable        float64      `json:"totalAvailable"`
	CrossMatching         bool         `json:"crossMatching"`
	RunnersVoidable       bool         `json:"runnersVoidable"`
	Version               int64        `json:"version"`
	Runners               []RunnerBook `json:"runners"`
	KeyLineDescription    *KeyLineDescription `json:"keyLineDescription,omitempty"`
}

type RunnerBook struct {
	SelectionID      int64              `json:"selectionId"`
	Handicap         float64            `json:"handicap"`
	Status           string             `json:"status"`
	AdjustmentFactor float64            `json:"adjustmentFactor"`
	LastPriceTraded  *float64           `json:"lastPriceTraded,omitempty"`
	TotalMatched     float64            `json:"totalMatched"`
	RemovalDate      *time.Time         `json:"removalDate,omitempty"`
	SP               *StartingPrices    `json:"sp,omitempty"`
	EX               *ExchangePrices    `json:"ex,omitempty"`
	Orders           []Order            `json:"orders,omitempty"`
	Matches          []Match            `json:"matches,omitempty"`
	MatchesByStrategy map[string][]Match `json:"matchesByStrategy,omitempty"`
}

type StartingPrices struct {
	NearPrice         *float64         `json:"nearPrice,omitempty"`
	FarPrice          *float64         `json:"farPrice,omitempty"`
	BackStakeTaken    []PriceSize      `json:"backStakeTaken,omitempty"`
	LayLiabilityTaken []PriceSize      `json:"layLiabilityTaken,omitempty"`
	ActualSP          *float64         `json:"actualSP,omitempty"`
}

type ExchangePrices struct {
	AvailableToBack []PriceSize `json:"availableToBack,omitempty"`
	AvailableToLay  []PriceSize `json:"availableToLay,omitempty"`
	TradedVolume    []PriceSize `json:"tradedVolume,omitempty"`
}

type PriceSize struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

type Order struct {
	BetID               string          `json:"betId"`
	OrderType           OrderType       `json:"orderType"`
	Status              string          `json:"status"`
	PersistenceType     PersistenceType `json:"persistenceType"`
	Side                Side            `json:"side"`
	Price               float64         `json:"price"`
	Size                float64         `json:"size"`
	BspLiability        float64         `json:"bspLiability"`
	PlacedDate          time.Time       `json:"placedDate"`
	AvgPriceMatched     *float64        `json:"avgPriceMatched,omitempty"`
	SizeMatched         float64         `json:"sizeMatched"`
	SizeRemaining       float64         `json:"sizeRemaining"`
	SizeLapsed          float64         `json:"sizeLapsed"`
	SizeCancelled       float64         `json:"sizeCancelled"`
	SizeVoided          float64         `json:"sizeVoided"`
	CustomerOrderRef    string          `json:"customerOrderRef,omitempty"`
	CustomerStrategyRef string          `json:"customerStrategyRef,omitempty"`
}

type Match struct {
	BetID     string    `json:"betId,omitempty"`
	MatchID   string    `json:"matchId,omitempty"`
	Side      Side      `json:"side"`
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	MatchDate time.Time `json:"matchDate"`
}

type KeyLineDescription struct {
	KeyLine []KeyLineSelection `json:"keyLine"`
}

type KeyLineSelection struct {
	SelectionID int64   `json:"selectionId"`
	Handicap    float64 `json:"handicap"`
}

// Execution Report Types
type PlaceExecutionReport struct {
	CustomerRef        string                   `json:"customerRef,omitempty"`
	Status             ExecutionReportStatus    `json:"status"`
	ErrorCode          *ExecutionReportErrorCode `json:"errorCode,omitempty"`
	MarketID           string                   `json:"marketId"`
	InstructionReports []PlaceInstructionReport `json:"instructionReports"`
}

type PlaceInstructionReport struct {
	Status           InstructionReportStatus      `json:"status"`
	ErrorCode        *InstructionReportErrorCode  `json:"errorCode,omitempty"`
	OrderStatus      *ExecutionReportStatus       `json:"orderStatus,omitempty"`
	Instruction      PlaceInstruction             `json:"instruction"`
	BetID            string                       `json:"betId,omitempty"`
	PlacedDate       *time.Time                   `json:"placedDate,omitempty"`
	AveragePriceMatched *float64                  `json:"averagePriceMatched,omitempty"`
	SizeMatched      float64                      `json:"sizeMatched"`
}

type ExecutionReportStatus string

const (
	ExecutionReportStatusSuccess ExecutionReportStatus = "SUCCESS"
	ExecutionReportStatusFailure ExecutionReportStatus = "FAILURE"
	ExecutionReportStatusProcessedWithErrors ExecutionReportStatus = "PROCESSED_WITH_ERRORS"
	ExecutionReportStatusTimeout ExecutionReportStatus = "TIMEOUT"
)

type ExecutionReportErrorCode string

type InstructionReportStatus string

const (
	InstructionReportStatusSuccess InstructionReportStatus = "SUCCESS"
	InstructionReportStatusFailure InstructionReportStatus = "FAILURE"
	InstructionReportStatusTimeout InstructionReportStatus = "TIMEOUT"
)

type InstructionReportErrorCode string

// Cancel/Replace/Update types
type CancelExecutionReport struct {
	CustomerRef        string                     `json:"customerRef,omitempty"`
	Status             ExecutionReportStatus      `json:"status"`
	ErrorCode          *ExecutionReportErrorCode  `json:"errorCode,omitempty"`
	MarketID           string                     `json:"marketId"`
	InstructionReports []CancelInstructionReport  `json:"instructionReports"`
}

type CancelInstructionReport struct {
	Status           InstructionReportStatus     `json:"status"`
	ErrorCode        *InstructionReportErrorCode `json:"errorCode,omitempty"`
	Instruction      CancelInstruction           `json:"instruction"`
	SizeCancelled    float64                     `json:"sizeCancelled"`
	CancelledDate    *time.Time                  `json:"cancelledDate,omitempty"`
}

type ReplaceExecutionReport struct {
	CustomerRef        string                     `json:"customerRef,omitempty"`
	Status             ExecutionReportStatus      `json:"status"`
	ErrorCode          *ExecutionReportErrorCode  `json:"errorCode,omitempty"`
	MarketID           string                     `json:"marketId"`
	InstructionReports []ReplaceInstructionReport `json:"instructionReports"`
}

type ReplaceInstructionReport struct {
	Status              InstructionReportStatus     `json:"status"`
	ErrorCode           *InstructionReportErrorCode `json:"errorCode,omitempty"`
	CancelInstructionReport *CancelInstructionReport `json:"cancelInstructionReport,omitempty"`
	PlaceInstructionReport  *PlaceInstructionReport  `json:"placeInstructionReport,omitempty"`
}

type UpdateExecutionReport struct {
	CustomerRef        string                    `json:"customerRef,omitempty"`
	Status             ExecutionReportStatus     `json:"status"`
	ErrorCode          *ExecutionReportErrorCode `json:"errorCode,omitempty"`
	MarketID           string                    `json:"marketId"`
	InstructionReports []UpdateInstructionReport `json:"instructionReports"`
}

type UpdateInstructionReport struct {
	Status      InstructionReportStatus     `json:"status"`
	ErrorCode   *InstructionReportErrorCode `json:"errorCode,omitempty"`
	Instruction UpdateInstruction           `json:"instruction"`
}

// Betting API Methods
func (c *RESTClient) ListMarketBook(ctx context.Context, marketIDs []string, priceProjection *PriceProjection, orderProjection *OrderProjection, matchProjection *string, includeOverallPosition *bool, partitionMatchedByStrategyRef *bool, customerStrategyRefs []string, currencyCode *string, locale *string, matchedSince *time.Time, betIDs []string) ([]MarketBook, error) {
	params := map[string]interface{}{
		"marketIds": marketIDs,
	}

	if priceProjection != nil {
		params["priceProjection"] = priceProjection
	}
	if orderProjection != nil {
		params["orderProjection"] = *orderProjection
	}
	if matchProjection != nil {
		params["matchProjection"] = *matchProjection
	}
	if includeOverallPosition != nil {
		params["includeOverallPosition"] = *includeOverallPosition
	}
	if partitionMatchedByStrategyRef != nil {
		params["partitionMatchedByStrategyRef"] = *partitionMatchedByStrategyRef
	}
	if len(customerStrategyRefs) > 0 {
		params["customerStrategyRefs"] = customerStrategyRefs
	}
	if currencyCode != nil {
		params["currencyCode"] = *currencyCode
	}
	if locale != nil {
		params["locale"] = *locale
	} else {
		params["locale"] = c.locale
	}
	if matchedSince != nil {
		params["matchedSince"] = *matchedSince
	}
	if len(betIDs) > 0 {
		params["betIds"] = betIDs
	}

	resp, err := c.makeBettingAPIRequest(ctx, "listMarketBook", params)
	if err != nil {
		return nil, err
	}

	var results []MarketBook
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &results); err != nil {
		return nil, fmt.Errorf("unmarshal market book: %w", err)
	}

	return results, nil
}

func (c *RESTClient) PlaceOrders(ctx context.Context, marketID string, instructions []PlaceInstruction, customerRef *string, marketVersion *int64, customerStrategyRef *string, async *bool) (*PlaceExecutionReport, error) {
	params := map[string]interface{}{
		"marketId":     marketID,
		"instructions": instructions,
		"locale":       c.locale,
	}

	if customerRef != nil {
		params["customerRef"] = *customerRef
	}
	if marketVersion != nil {
		params["marketVersion"] = *marketVersion
	}
	if customerStrategyRef != nil {
		params["customerStrategyRef"] = *customerStrategyRef
	}
	if async != nil {
		params["async"] = *async
	}

	resp, err := c.makeBettingAPIRequest(ctx, "placeOrders", params)
	if err != nil {
		return nil, err
	}

	var result PlaceExecutionReport
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &result); err != nil {
		return nil, fmt.Errorf("unmarshal place execution report: %w", err)
	}

	return &result, nil
}

func (c *RESTClient) CancelOrders(ctx context.Context, marketID string, instructions []CancelInstruction, customerRef *string) (*CancelExecutionReport, error) {
	if len(instructions) == 0 {
		return nil, fmt.Errorf("cancel instructions are required")
	}
	if len(instructions) > 60 {
		return nil, fmt.Errorf("maximum 60 cancel instructions allowed per request")
	}

	params := map[string]interface{}{
		"marketId":     marketID,
		"instructions": instructions,
		"locale":       c.locale,
	}

	if customerRef != nil {
		params["customerRef"] = *customerRef
	}

	resp, err := c.makeBettingAPIRequest(ctx, "cancelOrders", params)
	if err != nil {
		return nil, err
	}

	var result CancelExecutionReport
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &result); err != nil {
		return nil, fmt.Errorf("unmarshal cancel execution report: %w", err)
	}

	return &result, nil
}

func (c *RESTClient) ReplaceOrders(ctx context.Context, marketID string, instructions []ReplaceInstruction, customerRef *string, marketVersion *int64, async *bool) (*ReplaceExecutionReport, error) {
	if len(instructions) == 0 {
		return nil, fmt.Errorf("replace instructions are required")
	}
	if len(instructions) > 60 {
		return nil, fmt.Errorf("maximum 60 replace instructions allowed per request")
	}

	params := map[string]interface{}{
		"marketId":     marketID,
		"instructions": instructions,
		"locale":       c.locale,
	}

	if customerRef != nil {
		params["customerRef"] = *customerRef
	}
	if marketVersion != nil {
		params["marketVersion"] = *marketVersion
	}
	if async != nil {
		params["async"] = *async
	}

	resp, err := c.makeBettingAPIRequest(ctx, "replaceOrders", params)
	if err != nil {
		return nil, err
	}

	var result ReplaceExecutionReport
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &result); err != nil {
		return nil, fmt.Errorf("unmarshal replace execution report: %w", err)
	}

	return &result, nil
}

func (c *RESTClient) UpdateOrders(ctx context.Context, marketID string, instructions []UpdateInstruction, customerRef *string) (*UpdateExecutionReport, error) {
	if len(instructions) == 0 {
		return nil, fmt.Errorf("update instructions are required")
	}
	if len(instructions) > 60 {
		return nil, fmt.Errorf("maximum 60 update instructions allowed per request")
	}

	params := map[string]interface{}{
		"marketId":     marketID,
		"instructions": instructions,
		"locale":       c.locale,
	}

	if customerRef != nil {
		params["customerRef"] = *customerRef
	}

	resp, err := c.makeBettingAPIRequest(ctx, "updateOrders", params)
	if err != nil {
		return nil, err
	}

	var result UpdateExecutionReport
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &result); err != nil {
		return nil, fmt.Errorf("unmarshal update execution report: %w", err)
	}

	return &result, nil
}