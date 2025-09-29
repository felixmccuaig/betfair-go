package betfair

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	AuthURLInteractiveLogin = "https://identitysso.betfair.com.au:443/api/login"
	AuthURLBotLogin        = "https://identitysso-api.betfair.com.au:443/api/certlogin"
	AuthURLLogout          = "https://identitysso.betfair.com.au:443/api/logout"
	AuthURLKeepAlive       = "https://identitysso.betfair.com.au:443/api/keepAlive"
	BettingURLExchange     = "https://api.betfair.com:443/exchange/betting/json-rpc/v1"
	AccountURLAccounts     = "https://api.betfair.com/exchange/account/json-rpc/v1"
)

type RESTClient struct {
	appKey     string
	sessionKey string
	locale     string
	httpClient *http.Client
}

func NewRESTClient(appKey, sessionKey, locale string) *RESTClient {
	return &RESTClient{
		appKey:     appKey,
		sessionKey: sessionKey,
		locale:     locale,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *RESTClient) UpdateSessionKey(sessionKey string) {
	c.sessionKey = sessionKey
}

type JSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int64       `json:"id"`
}

type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result"`
	Error   *RPCError   `json:"error,omitempty"`
	ID      int64       `json:"id"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (c *RESTClient) makeRequest(ctx context.Context, requestURL, method string, data interface{}) (*http.Response, error) {
	var body io.Reader
	var contentType string

	if data != nil {
		switch v := data.(type) {
		case url.Values:
			contentType = "application/x-www-form-urlencoded"
			body = strings.NewReader(v.Encode())
		default:
			contentType = "application/json"
			jsonData, err := json.Marshal(data)
			if err != nil {
				return nil, fmt.Errorf("marshal request data: %w", err)
			}
			body = bytes.NewReader(jsonData)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.appKey != "" {
		req.Header.Set("X-Application", c.appKey)
	}
	if c.sessionKey != "" {
		req.Header.Set("X-Authentication", c.sessionKey)
	}

	return c.httpClient.Do(req)
}

func (c *RESTClient) makeBettingAPIRequest(ctx context.Context, method string, params interface{}) (*JSONRPCResponse, error) {
	requestPayload := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  fmt.Sprintf("SportsAPING/v1.0/%s", method),
		Params:  params,
		ID:      time.Now().UnixNano(),
	}

	resp, err := c.makeRequest(ctx, BettingURLExchange, "POST", requestPayload)
	if err != nil {
		return nil, fmt.Errorf("make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	var rpcResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("API error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return &rpcResp, nil
}

func (c *RESTClient) makeAccountAPIRequest(ctx context.Context, method string, params interface{}) (*JSONRPCResponse, error) {
	requestPayload := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  fmt.Sprintf("AccountAPING/v1.0/%s", method),
		Params:  params,
		ID:      time.Now().UnixNano(),
	}

	resp, err := c.makeRequest(ctx, AccountURLAccounts, "POST", requestPayload)
	if err != nil {
		return nil, fmt.Errorf("make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	var rpcResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("API error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return &rpcResp, nil
}

// Market Data Types
type MarketFilter struct {
	TextQuery          string                 `json:"textQuery,omitempty"`
	ExchangeIds        []string              `json:"exchangeIds,omitempty"`
	EventTypeIds       []string              `json:"eventTypeIds,omitempty"`
	EventIds           []string              `json:"eventIds,omitempty"`
	CompetitionIds     []string              `json:"competitionIds,omitempty"`
	MarketIds          []string              `json:"marketIds,omitempty"`
	Venues             []string              `json:"venues,omitempty"`
	BspOnly            *bool                 `json:"bspOnly,omitempty"`
	TurnInPlayEnabled  *bool                 `json:"turnInPlayEnabled,omitempty"`
	InPlayOnly         *bool                 `json:"inPlayOnly,omitempty"`
	MarketBettingTypes []string              `json:"marketBettingTypes,omitempty"`
	MarketCountries    []string              `json:"marketCountries,omitempty"`
	MarketTypeCodes    []string              `json:"marketTypeCodes,omitempty"`
	MarketStartTime    *TimeRange            `json:"marketStartTime,omitempty"`
	WithOrders         []string              `json:"withOrders,omitempty"`
	RaceTypes          []string              `json:"raceTypes,omitempty"`
}

type TimeRange struct {
	From *time.Time `json:"from,omitempty"`
	To   *time.Time `json:"to,omitempty"`
}

type MarketSort string

const (
	MarketSortMinimumTraded     MarketSort = "MINIMUM_TRADED"
	MarketSortMaximumTraded     MarketSort = "MAXIMUM_TRADED"
	MarketSortMinimumAvailable  MarketSort = "MINIMUM_AVAILABLE"
	MarketSortMaximumAvailable  MarketSort = "MAXIMUM_AVAILABLE"
	MarketSortFirstToStart      MarketSort = "FIRST_TO_START"
	MarketSortLastToStart       MarketSort = "LAST_TO_START"
)

type MarketProjection string

const (
	MarketProjectionCompetition        MarketProjection = "COMPETITION"
	MarketProjectionEvent              MarketProjection = "EVENT"
	MarketProjectionEventType          MarketProjection = "EVENT_TYPE"
	MarketProjectionMarketStartTime    MarketProjection = "MARKET_START_TIME"
	MarketProjectionMarketDescription  MarketProjection = "MARKET_DESCRIPTION"
	MarketProjectionRunnerDescription  MarketProjection = "RUNNER_DESCRIPTION"
	MarketProjectionRunnerMetadata     MarketProjection = "RUNNER_METADATA"
)

type MarketCatalogue struct {
	MarketID        string               `json:"marketId"`
	MarketName      string               `json:"marketName"`
	MarketStartTime *time.Time           `json:"marketStartTime,omitempty"`
	Description     *MarketDescription   `json:"description,omitempty"`
	TotalMatched    float64              `json:"totalMatched,omitempty"`
	Runners         []RunnerCatalog      `json:"runners,omitempty"`
	EventType       *EventType           `json:"eventType,omitempty"`
	Competition     *Competition         `json:"competition,omitempty"`
	Event           *Event               `json:"event,omitempty"`
}

type MarketDescription struct {
	PersistenceEnabled    bool        `json:"persistenceEnabled"`
	BspMarket            bool        `json:"bspMarket"`
	MarketTime           *time.Time  `json:"marketTime,omitempty"`
	SuspendTime          *time.Time  `json:"suspendTime,omitempty"`
	SettleTime           *time.Time  `json:"settleTime,omitempty"`
	BettingType          string      `json:"bettingType,omitempty"`
	TurnInPlayEnabled    bool        `json:"turnInPlayEnabled"`
	MarketType           string      `json:"marketType,omitempty"`
	Regulator            string      `json:"regulator,omitempty"`
	MarketBaseRate       float64     `json:"marketBaseRate,omitempty"`
	DiscountAllowed      bool        `json:"discountAllowed"`
	Wallet               string      `json:"wallet,omitempty"`
	Rules                string      `json:"rules,omitempty"`
	RulesHasDate         bool        `json:"rulesHasDate"`
	EachWayDivisor       float64     `json:"eachWayDivisor,omitempty"`
	Clarifications       string      `json:"clarifications,omitempty"`
	LineRangeInfo        *LineRangeInfo `json:"lineRangeInfo,omitempty"`
	RaceType             string      `json:"raceType,omitempty"`
	PriceLadderDescription *PriceLadderDescription `json:"priceLadderDescription,omitempty"`
}

type LineRangeInfo struct {
	MaxUnitValue float64 `json:"maxUnitValue"`
	MinUnitValue float64 `json:"minUnitValue"`
	Interval     float64 `json:"interval"`
	MarketUnit   string  `json:"marketUnit"`
}

type PriceLadderDescription struct {
	Type string `json:"type"`
}

type RunnerCatalog struct {
	SelectionID  int64             `json:"selectionId"`
	RunnerName   string            `json:"runnerName"`
	Handicap     float64           `json:"handicap"`
	SortPriority int               `json:"sortPriority"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type EventType struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Competition struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Event struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	CountryCode string     `json:"countryCode,omitempty"`
	Timezone    string     `json:"timezone,omitempty"`
	Venue       string     `json:"venue,omitempty"`
	OpenDate    *time.Time `json:"openDate,omitempty"`
}

// Market Data Functions
func (c *RESTClient) ListMarketCatalogue(ctx context.Context, filter MarketFilter, marketProjection []MarketProjection, sort MarketSort, maxResults int) ([]MarketCatalogue, error) {
	params := map[string]interface{}{
		"filter":           filter,
		"marketProjection": marketProjection,
		"sort":             sort,
		"maxResults":       maxResults,
		"locale":           c.locale,
	}

	resp, err := c.makeBettingAPIRequest(ctx, "listMarketCatalogue", params)
	if err != nil {
		return nil, err
	}

	var results []MarketCatalogue
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &results); err != nil {
		return nil, fmt.Errorf("unmarshal market catalogue: %w", err)
	}

	return results, nil
}

func (c *RESTClient) ListEventTypes(ctx context.Context, filter MarketFilter) ([]EventTypeResult, error) {
	params := map[string]interface{}{
		"filter": filter,
		"locale": c.locale,
	}

	resp, err := c.makeBettingAPIRequest(ctx, "listEventTypes", params)
	if err != nil {
		return nil, err
	}

	var results []EventTypeResult
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &results); err != nil {
		return nil, fmt.Errorf("unmarshal event types: %w", err)
	}

	return results, nil
}

func (c *RESTClient) ListCompetitions(ctx context.Context, filter MarketFilter) ([]CompetitionResult, error) {
	params := map[string]interface{}{
		"filter": filter,
		"locale": c.locale,
	}

	resp, err := c.makeBettingAPIRequest(ctx, "listCompetitions", params)
	if err != nil {
		return nil, err
	}

	var results []CompetitionResult
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &results); err != nil {
		return nil, fmt.Errorf("unmarshal competitions: %w", err)
	}

	return results, nil
}

func (c *RESTClient) ListEvents(ctx context.Context, filter MarketFilter) ([]EventResult, error) {
	params := map[string]interface{}{
		"filter": filter,
		"locale": c.locale,
	}

	resp, err := c.makeBettingAPIRequest(ctx, "listEvents", params)
	if err != nil {
		return nil, err
	}

	var results []EventResult
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &results); err != nil {
		return nil, fmt.Errorf("unmarshal events: %w", err)
	}

	return results, nil
}

func (c *RESTClient) ListMarketTypes(ctx context.Context, filter MarketFilter) ([]MarketTypeResult, error) {
	params := map[string]interface{}{
		"filter": filter,
		"locale": c.locale,
	}

	resp, err := c.makeBettingAPIRequest(ctx, "listMarketTypes", params)
	if err != nil {
		return nil, err
	}

	var results []MarketTypeResult
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &results); err != nil {
		return nil, fmt.Errorf("unmarshal market types: %w", err)
	}

	return results, nil
}

func (c *RESTClient) ListCountries(ctx context.Context, filter MarketFilter) ([]CountryCodeResult, error) {
	params := map[string]interface{}{
		"filter": filter,
		"locale": c.locale,
	}

	resp, err := c.makeBettingAPIRequest(ctx, "listCountries", params)
	if err != nil {
		return nil, err
	}

	var results []CountryCodeResult
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &results); err != nil {
		return nil, fmt.Errorf("unmarshal countries: %w", err)
	}

	return results, nil
}

func (c *RESTClient) ListVenues(ctx context.Context, filter MarketFilter) ([]VenueResult, error) {
	params := map[string]interface{}{
		"filter": filter,
		"locale": c.locale,
	}

	resp, err := c.makeBettingAPIRequest(ctx, "listVenues", params)
	if err != nil {
		return nil, err
	}

	var results []VenueResult
	resultBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	if err := json.Unmarshal(resultBytes, &results); err != nil {
		return nil, fmt.Errorf("unmarshal venues: %w", err)
	}

	return results, nil
}

// Result types for list operations
type EventTypeResult struct {
	EventType   EventType `json:"eventType"`
	MarketCount int       `json:"marketCount"`
}

type CompetitionResult struct {
	Competition      Competition `json:"competition"`
	MarketCount      int         `json:"marketCount"`
	CompetitionRegion string     `json:"competitionRegion,omitempty"`
}

type EventResult struct {
	Event       Event `json:"event"`
	MarketCount int   `json:"marketCount"`
}

type MarketTypeResult struct {
	MarketType  string `json:"marketType"`
	MarketCount int    `json:"marketCount"`
}

type CountryCodeResult struct {
	CountryCode string `json:"countryCode"`
	MarketCount int    `json:"marketCount"`
}

type VenueResult struct {
	Venue       string `json:"venue"`
	MarketCount int    `json:"marketCount"`
}