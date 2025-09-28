package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

const (
	BetfairStreamHost    = "stream-api.betfair.com"
	BetfairStreamPort    = "443"
	BetfairStreamAddress = BetfairStreamHost + ":" + BetfairStreamPort
)

type StreamConn struct {
	conn   *tls.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewStreamConn(conn *tls.Conn) *StreamConn {
	return &StreamConn{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

func (s *StreamConn) WriteJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := s.writer.Write(data); err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *StreamConn) ReadMessage() ([]byte, error) {
	for {
		line, err := s.reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}
		if isGzip(trimmed) {
			payload, err := ungzip(trimmed)
			if err != nil {
				return nil, err
			}
			return bytes.TrimSpace(payload), nil
		}
		return trimmed, nil
	}
}

func (s *StreamConn) Close() error {
	return s.conn.Close()
}

func (s *StreamConn) SetReadDeadline(t time.Time) error {
	return s.conn.SetReadDeadline(t)
}

type StreamClient struct {
	appKey       string
	sessionToken string
	heartbeatMs  int
	logger       zerolog.Logger
	authenticator *Authenticator
}

func NewStreamClient(appKey, sessionToken string, heartbeatMs int, logger zerolog.Logger, auth *Authenticator) *StreamClient {
	return &StreamClient{
		appKey:       appKey,
		sessionToken: sessionToken,
		heartbeatMs:  heartbeatMs,
		logger:       logger,
		authenticator: auth,
	}
}

func (sc *StreamClient) Dial() (*StreamConn, error) {
	tlsConf := &tls.Config{
		ServerName: BetfairStreamHost,
		MinVersion: tls.VersionTLS12,
	}

	sc.logger.Debug().Str("address", BetfairStreamAddress).Msg("connecting to Betfair stream")
	conn, err := tls.Dial("tcp", BetfairStreamAddress, tlsConf)
	if err != nil {
		return nil, fmt.Errorf("dial betfair stream: %w", err)
	}

	sc.logger.Debug().Msg("TLS connection established")
	return NewStreamConn(conn), nil
}

func (sc *StreamClient) Authenticate(stream *StreamConn) error {
	auth := map[string]any{
		"op":      "authentication",
		"id":      1,
		"appKey":  sc.appKey,
		"session": sc.sessionToken,
	}

	sc.logger.Debug().Msg("sending authentication request")
	if err := stream.WriteJSON(auth); err != nil {
		return fmt.Errorf("send authentication: %w", err)
	}

	if err := stream.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	defer stream.SetReadDeadline(time.Time{})

	for {
		payload, err := stream.ReadMessage()
		if err != nil {
			sc.logger.Error().Err(err).Msg("failed to read message during authentication")
			return fmt.Errorf("read authentication response: %w", err)
		}

		op := ExtractOp(payload)
		sc.logger.Debug().Str("op", op).RawJSON("payload", payload).Msg("received message during authentication")

		if op == "connection" {
			sc.logger.Info().RawJSON("connection", payload).Msg("received connection info")
			continue
		}
		if op == "heartbeat" {
			sc.logger.Debug().Msg("received heartbeat while authenticating")
			continue
		}

		if err := validateAck("authentication", payload); err != nil {
			sc.logger.Error().Err(err).RawJSON("payload", payload).Msg("authentication validation failed")

			if IsInvalidSessionError(err) && sc.authenticator != nil {
				sc.logger.Info().Msg("session token expired, attempting to refresh")
				newToken, refreshErr := sc.authenticator.Login()
				if refreshErr != nil {
					return fmt.Errorf("failed to refresh session token: %w", refreshErr)
				}
				sc.sessionToken = newToken
				return fmt.Errorf("session refreshed, retry connection: %w", err)
			}
			return err
		}

		sc.logger.Info().Msg("authenticated with Betfair stream API")
		return nil
	}
}

func (sc *StreamClient) RequestHeartbeat(stream *StreamConn) error {
	heartbeat := map[string]any{
		"op":          "heartbeat",
		"id":          2,
		"heartbeatMs": sc.heartbeatMs,
	}
	if err := stream.WriteJSON(heartbeat); err != nil {
		return fmt.Errorf("send heartbeat request: %w", err)
	}
	return nil
}

// MarketFilter is defined in rest_api.go to avoid duplication

func (sc *StreamClient) Subscribe(stream *StreamConn, filter MarketFilter, initialClk, clk string) error {
	marketFilter := map[string]any{}

	if len(filter.MarketIds) > 0 {
		marketFilter["marketIds"] = filter.MarketIds
	}
	if len(filter.EventTypeIds) > 0 {
		marketFilter["eventTypeIds"] = filter.EventTypeIds
	}
	if len(filter.MarketCountries) > 0 {
		marketFilter["countryCodes"] = filter.MarketCountries
	}
	if len(filter.MarketTypeCodes) > 0 {
		marketFilter["marketTypes"] = filter.MarketTypeCodes
	}

	subscription := map[string]any{
		"op":           "marketSubscription",
		"id":           3,
		"marketFilter": marketFilter,
		"marketDataFilter": map[string]any{
			"fields": []string{
				"EX_ALL_OFFERS",
				"EX_TRADED",
				"EX_MARKET_DEF",
				"EX_LTP",
				"EX_TRADED_VOL",
				"SP_TRADED",
				"SP_PROJECTED",
			},
		},
	}

	if initialClk != "" {
		subscription["initialClk"] = initialClk
		sc.logger.Info().Str("initialClk", initialClk).Msg("using stored initialClk for fast recovery")
	}
	if clk != "" {
		subscription["clk"] = clk
		sc.logger.Info().Str("clk", clk).Msg("using stored clk for fast recovery")
	}

	if err := stream.WriteJSON(subscription); err != nil {
		return fmt.Errorf("send subscription: %w", err)
	}

	return sc.waitForSubscriptionAck(stream)
}

func (sc *StreamClient) waitForSubscriptionAck(stream *StreamConn) error {
	if err := stream.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	defer stream.SetReadDeadline(time.Time{})

	for {
		payload, err := stream.ReadMessage()
		if err != nil {
			sc.logger.Error().Err(err).Msg("failed to read message while waiting for subscription ack")
			return fmt.Errorf("waiting subscription ack: %w", err)
		}

		op := ExtractOp(payload)
		sc.logger.Debug().Str("op", op).RawJSON("payload", payload).Msg("received message while waiting for subscription ack")

		if op == "heartbeat" {
			sc.logger.Debug().Msg("received heartbeat while waiting for subscription ack")
			continue
		}

		if err := validateAck("marketSubscription", payload); err == nil {
			sc.logger.Info().Msg("market subscription confirmed")
			return nil
		}

		if err := validateAck("status", payload); err == nil {
			sc.logger.Info().Msg("received status acknowledgment")
			return nil
		}

		sc.logger.Debug().RawJSON("message", payload).Msg("non-ack message while waiting for subscription")
	}
}

func validateAck(expectedOp string, raw []byte) error {
	type ack struct {
		Op         string `json:"op"`
		Status     string `json:"status"`
		StatusCode string `json:"statusCode"`
		Error      string `json:"errorMessage"`
		ErrorCode  string `json:"errorCode"`
	}

	var a ack
	if err := json.Unmarshal(raw, &a); err != nil {
		return fmt.Errorf("decode ack: %w", err)
	}

	if a.Op != expectedOp && a.Op != "status" {
		return fmt.Errorf("unexpected op %q (want %q)", a.Op, expectedOp)
	}

	status := strings.ToUpper(firstNonEmpty(a.Status, a.StatusCode))
	if status != "SUCCESS" {
		errMsg := firstNonEmpty(a.Error, a.ErrorCode, "unknown error")
		return fmt.Errorf("%s failed: %s", expectedOp, errMsg)
	}

	return nil
}

func ungzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create gzip reader: %w", err)
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func isGzip(data []byte) bool {
	return len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b
}