package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// CreatePlaceInstruction creates a simple place instruction for a back or lay bet
func CreatePlaceInstruction(selectionID int64, side Side, price, size float64, persistenceType PersistenceType) PlaceInstruction {
	return PlaceInstruction{
		OrderType:   OrderTypeLimit,
		SelectionID: selectionID,
		Side:        side,
		LimitOrder: &LimitOrder{
			Size:            size,
			Price:           price,
			PersistenceType: persistenceType,
		},
	}
}

// CreateCancelInstruction creates a cancel instruction for a specific bet
func CreateCancelInstruction(betID string, sizeReduction *float64) CancelInstruction {
	return CancelInstruction{
		BetID:         betID,
		SizeReduction: sizeReduction,
	}
}

// CreateReplaceInstruction creates a replace instruction for a specific bet
func CreateReplaceInstruction(betID string, newPrice float64) ReplaceInstruction {
	return ReplaceInstruction{
		BetID:    betID,
		NewPrice: newPrice,
	}
}

// CreateUpdateInstruction creates an update instruction for a specific bet
func CreateUpdateInstruction(betID string, newPersistenceType PersistenceType) UpdateInstruction {
	return UpdateInstruction{
		BetID:              betID,
		NewPersistenceType: newPersistenceType,
	}
}

// ValidateBetID validates if a bet ID is in correct format
func ValidateBetID(betID string) bool {
	// Bet IDs are typically numeric strings
	matched, _ := regexp.MatchString(`^\d+$`, betID)
	return matched
}

// ValidateMarketID validates if a market ID is in correct format
func ValidateMarketID(marketID string) bool {
	// Market IDs follow pattern like "1.12345678"
	matched, _ := regexp.MatchString(`^1\.\d+$`, marketID)
	return matched
}

// CalculateBackProfit calculates potential profit for a back bet
func CalculateBackProfit(stake, odds float64) float64 {
	return stake * (odds - 1)
}

// CalculateLayLiability calculates liability for a lay bet
func CalculateLayLiability(stake, odds float64) float64 {
	return stake * (odds - 1)
}

// ValidateOrderParameters validates order parameters for common issues
func ValidateOrderParameters(marketID string, selectionID int64, price, size float64) error {
	if !ValidateMarketID(marketID) {
		return fmt.Errorf("invalid market ID format: %s", marketID)
	}

	if selectionID <= 0 {
		return fmt.Errorf("selection ID must be a positive integer: %d", selectionID)
	}

	if price < 1.01 || price > 1000 {
		return fmt.Errorf("price must be between 1.01 and 1000: %f", price)
	}

	if size < 0.01 {
		return fmt.Errorf("size must be at least 0.01: %f", size)
	}

	if size > 100000 {
		return fmt.Errorf("size cannot exceed 100,000: %f", size)
	}

	return nil
}

// StandardizeLocation standardizes location names for Betfair API consistency
func StandardizeLocation(location string) string {
	// Basic location standardization
	location = strings.TrimSpace(location)
	location = strings.ToLower(location)

	// Remove special characters
	reg := regexp.MustCompile(`[^a-z0-9\s]`)
	location = reg.ReplaceAllString(location, "")

	// Normalize whitespace
	reg = regexp.MustCompile(`\s+`)
	location = reg.ReplaceAllString(location, " ")

	// Capitalize words
	words := strings.Split(location, " ")
	for i, word := range words {
		if len(word) > 0 {
			words[i] = strings.ToUpper(word[:1]) + word[1:]
		}
	}

	return strings.Join(words, " ")
}

// RoundToValidPrice rounds a price to valid Betfair price increments
func RoundToValidPrice(price float64) float64 {
	// Betfair uses specific price increments
	switch {
	case price >= 1.01 && price < 2:
		return float64(int(price*100+0.5)) / 100 // Round to 0.01
	case price >= 2 && price < 3:
		return float64(int(price*50+0.5)) / 50 // Round to 0.02
	case price >= 3 && price < 4:
		return float64(int(price*20+0.5)) / 20 // Round to 0.05
	case price >= 4 && price < 6:
		return float64(int(price*10+0.5)) / 10 // Round to 0.1
	case price >= 6 && price < 10:
		return float64(int(price*5+0.5)) / 5 // Round to 0.2
	case price >= 10 && price < 20:
		return float64(int(price*2+0.5)) / 2 // Round to 0.5
	case price >= 20 && price < 30:
		return float64(int(price+0.5)) // Round to 1
	case price >= 30 && price < 50:
		return float64(int(price/2+0.5)) * 2 // Round to 2
	case price >= 50 && price < 100:
		return float64(int(price/5+0.5)) * 5 // Round to 5
	case price >= 100 && price <= 1000:
		return float64(int(price/10+0.5)) * 10 // Round to 10
	default:
		return price
	}
}

// CreateTimeRange creates a time range for filtering
func CreateTimeRange(from, to *time.Time) *TimeRange {
	return &TimeRange{
		From: from,
		To:   to,
	}
}

// CreateMarketFilter creates a basic market filter
func CreateMarketFilter() *MarketFilter {
	return &MarketFilter{}
}

// WithEventTypeIDs adds event type IDs to a market filter
func (mf *MarketFilter) WithEventTypeIDs(eventTypeIDs []string) *MarketFilter {
	mf.EventTypeIds = eventTypeIDs
	return mf
}

// WithEventIDs adds event IDs to a market filter
func (mf *MarketFilter) WithEventIDs(eventIDs []string) *MarketFilter {
	mf.EventIds = eventIDs
	return mf
}

// WithMarketIDs adds market IDs to a market filter
func (mf *MarketFilter) WithMarketIDs(marketIDs []string) *MarketFilter {
	mf.MarketIds = marketIDs
	return mf
}

// WithCompetitionIDs adds competition IDs to a market filter
func (mf *MarketFilter) WithCompetitionIDs(competitionIDs []string) *MarketFilter {
	mf.CompetitionIds = competitionIDs
	return mf
}

// WithMarketCountries adds market countries to a market filter
func (mf *MarketFilter) WithMarketCountries(countries []string) *MarketFilter {
	mf.MarketCountries = countries
	return mf
}

// WithMarketTypeCodes adds market type codes to a market filter
func (mf *MarketFilter) WithMarketTypeCodes(marketTypes []string) *MarketFilter {
	mf.MarketTypeCodes = marketTypes
	return mf
}

// WithVenues adds venues to a market filter
func (mf *MarketFilter) WithVenues(venues []string) *MarketFilter {
	mf.Venues = venues
	return mf
}

// WithMarketStartTime adds market start time range to a market filter
func (mf *MarketFilter) WithMarketStartTime(timeRange *TimeRange) *MarketFilter {
	mf.MarketStartTime = timeRange
	return mf
}

// WithInPlayOnly sets the in-play only filter
func (mf *MarketFilter) WithInPlayOnly(inPlayOnly bool) *MarketFilter {
	mf.InPlayOnly = &inPlayOnly
	return mf
}

// WithBSPOnly sets the BSP only filter
func (mf *MarketFilter) WithBSPOnly(bspOnly bool) *MarketFilter {
	mf.BspOnly = &bspOnly
	return mf
}

// WithTurnInPlayEnabled sets the turn in play enabled filter
func (mf *MarketFilter) WithTurnInPlayEnabled(enabled bool) *MarketFilter {
	mf.TurnInPlayEnabled = &enabled
	return mf
}

// CreatePriceProjection creates a basic price projection
func CreatePriceProjection(priceData []PriceData) *PriceProjection {
	return &PriceProjection{
		PriceData: priceData,
	}
}

// WithBestOffersOverrides adds best offers overrides to price projection
func (pp *PriceProjection) WithBestOffersOverrides(overrides *ExBestOffersOverrides) *PriceProjection {
	pp.ExBestOffersOverrides = overrides
	return pp
}

// WithVirtualise sets virtualise flag for price projection
func (pp *PriceProjection) WithVirtualise(virtualise bool) *PriceProjection {
	pp.Virtualise = &virtualise
	return pp
}

// WithRolloverStakes sets rollover stakes flag for price projection
func (pp *PriceProjection) WithRolloverStakes(rollover bool) *PriceProjection {
	pp.RolloverStakes = &rollover
	return pp
}

// GetBestBackPrice gets the best available back price from a runner
func GetBestBackPrice(runner RunnerBook) *float64 {
	if runner.EX != nil && len(runner.EX.AvailableToBack) > 0 {
		return &runner.EX.AvailableToBack[0].Price
	}
	return nil
}

// GetBestLayPrice gets the best available lay price from a runner
func GetBestLayPrice(runner RunnerBook) *float64 {
	if runner.EX != nil && len(runner.EX.AvailableToLay) > 0 {
		return &runner.EX.AvailableToLay[0].Price
	}
	return nil
}

// GetBestBackSize gets the best available back size from a runner
func GetBestBackSize(runner RunnerBook) *float64 {
	if runner.EX != nil && len(runner.EX.AvailableToBack) > 0 {
		return &runner.EX.AvailableToBack[0].Size
	}
	return nil
}

// GetBestLaySize gets the best available lay size from a runner
func GetBestLaySize(runner RunnerBook) *float64 {
	if runner.EX != nil && len(runner.EX.AvailableToLay) > 0 {
		return &runner.EX.AvailableToLay[0].Size
	}
	return nil
}

// IsRunnerWinner checks if a runner is marked as winner
func IsRunnerWinner(runner RunnerBook) bool {
	return runner.Status == "WINNER"
}

// IsRunnerActive checks if a runner is active (not removed/voided)
func IsRunnerActive(runner RunnerBook) bool {
	return runner.Status == "ACTIVE"
}

// CalculateTotalVolume calculates total traded volume for a runner
func CalculateTotalVolume(runner RunnerBook) float64 {
	total := 0.0
	if runner.EX != nil {
		for _, volume := range runner.EX.TradedVolume {
			total += volume.Size
		}
	}
	return total
}

// FormatPrice formats a price for display
func FormatPrice(price float64) string {
	if price >= 100 {
		return fmt.Sprintf("%.0f", price)
	} else if price >= 10 {
		return fmt.Sprintf("%.1f", price)
	} else {
		return fmt.Sprintf("%.2f", price)
	}
}

// FormatSize formats a size for display
func FormatSize(size float64) string {
	if size >= 1000 {
		return fmt.Sprintf("%.0fk", size/1000)
	} else if size >= 100 {
		return fmt.Sprintf("%.0f", size)
	} else {
		return fmt.Sprintf("%.2f", size)
	}
}