package betfair

import (
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

type Config struct {
	AppKey       string
	SessionToken string
	MarketIDs    []string
	EventTypeID  string
	CountryCode  string
	MarketType   string
	OutputPath   string
	S3Bucket     string
	S3BasePath   string
	HeartbeatMs  int
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) LoadFromEnv() error {
	c.AppKey = strings.TrimSpace(os.Getenv("BETFAIR_APP_KEY"))
	username := strings.TrimSpace(os.Getenv("BETFAIR_USERNAME"))
	password := strings.TrimSpace(os.Getenv("BETFAIR_PASSWORD"))
	c.SessionToken = strings.TrimSpace(os.Getenv("BETFAIR_SESSION_TOKEN"))
	c.S3Bucket = strings.TrimSpace(os.Getenv("S3_BUCKET"))
	c.S3BasePath = strings.TrimSpace(os.Getenv("S3_BASE_PATH"))

	markets := strings.TrimSpace(os.Getenv("MARKET_IDS"))
	c.EventTypeID = strings.TrimSpace(os.Getenv("EVENT_TYPE_ID"))
	c.CountryCode = strings.TrimSpace(os.Getenv("COUNTRY_CODE"))
	c.MarketType = strings.TrimSpace(os.Getenv("MARKET_TYPE"))
	c.OutputPath = strings.TrimSpace(os.Getenv("OUTPUT_PATH"))

	c.HeartbeatMs = 5000
	if h := strings.TrimSpace(os.Getenv("HEARTBEAT_MS")); h != "" {
		if parsed, err := strconv.Atoi(h); err == nil && parsed > 0 {
			c.HeartbeatMs = parsed
		}
	}

	if c.AppKey == "" {
		log.Fatal().Msg("BETFAIR_APP_KEY environment variable is required")
	}

	if c.SessionToken == "" {
		if username == "" || password == "" {
			log.Fatal().Msg("BETFAIR_USERNAME and BETFAIR_PASSWORD must be set or provide BETFAIR_SESSION_TOKEN")
		}
		auth := NewAuthenticator(c.AppKey, username, password)
		var err error
		c.SessionToken, err = auth.Login()
		if err != nil {
			log.Fatal().Err(err).Msg("interactive Betfair login failed")
		}
		log.Info().Msg("obtained session token via interactive login")
	}

	_ = os.Setenv("BETFAIR_SESSION_TOKEN", c.SessionToken)

	if markets != "" {
		c.MarketIDs = splitAndClean(markets)
	} else if c.EventTypeID == "" {
		log.Fatal().Msg("either MARKET_IDS or EVENT_TYPE_ID environment variable must be provided")
	}

	if c.HeartbeatMs <= 0 {
		c.HeartbeatMs = 5000
	}

	return nil
}

func (c *Config) GetMarketFilter() MarketFilter {
	filter := MarketFilter{
		MarketIds: c.MarketIDs,
	}

	if c.EventTypeID != "" {
		filter.EventTypeIds = []string{c.EventTypeID}
	}
	if c.CountryCode != "" {
		filter.MarketCountries = []string{c.CountryCode}
	}
	if c.MarketType != "" {
		filter.MarketTypeCodes = []string{c.MarketType}
	}

	return filter
}

func splitAndClean(csv string) []string {
	parts := strings.Split(csv, ",")
	cleaned := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	return cleaned
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}