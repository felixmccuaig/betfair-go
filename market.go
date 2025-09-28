package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type EventInfo struct {
	EventID string
	Date    time.Time
	Year    string
	Month   string
	Day     string
}

type MarketProcessor struct{}

func NewMarketProcessor() *MarketProcessor {
	return &MarketProcessor{}
}

func ExtractOp(raw []byte) string {
	var base struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(raw, &base); err == nil {
		return base.Op
	}
	return ""
}

func ExtractMarketID(raw []byte) string {
	var mcm struct {
		MC []struct {
			ID string `json:"id"`
		} `json:"mc"`
	}
	if err := json.Unmarshal(raw, &mcm); err == nil && len(mcm.MC) > 0 {
		return mcm.MC[0].ID
	}
	return ""
}

func ExtractChangeType(raw []byte) string {
	var base struct {
		CT string `json:"ct"`
	}
	if err := json.Unmarshal(raw, &base); err == nil {
		return base.CT
	}
	return ""
}

func ExtractMarketStatus(raw []byte) string {
	var mcm struct {
		MC []struct {
			MarketDefinition struct {
				Status string `json:"status"`
			} `json:"marketDefinition"`
		} `json:"mc"`
	}

	if err := json.Unmarshal(raw, &mcm); err != nil {
		return ""
	}

	if len(mcm.MC) > 0 {
		return mcm.MC[0].MarketDefinition.Status
	}
	return ""
}

func ExtractEventInfo(raw []byte) (*EventInfo, error) {
	var mcm struct {
		MC []struct {
			MarketDefinition struct {
				EventID  string    `json:"eventId"`
				OpenDate time.Time `json:"openDate"`
			} `json:"marketDefinition"`
		} `json:"mc"`
	}

	if err := json.Unmarshal(raw, &mcm); err != nil {
		return nil, err
	}

	if len(mcm.MC) == 0 || mcm.MC[0].MarketDefinition.EventID == "" {
		return nil, fmt.Errorf("no event information found")
	}

	info := &EventInfo{
		EventID: mcm.MC[0].MarketDefinition.EventID,
		Date:    mcm.MC[0].MarketDefinition.OpenDate,
	}

	info.Year = strconv.Itoa(info.Date.Year())
	info.Month = info.Date.Format("Jan")
	info.Day = strconv.Itoa(info.Date.Day())

	return info, nil
}

func ExtractAndStoreClock(raw []byte) (initialClk, clk string) {
	var clockMsg struct {
		InitialClk string `json:"initialClk"`
		Clk        string `json:"clk"`
	}
	if err := json.Unmarshal(raw, &clockMsg); err == nil {
		return clockMsg.InitialClk, clockMsg.Clk
	}
	return "", ""
}

func IsMarketSettled(status string) bool {
	return status == "CLOSED"
}

func RemoveIDField(raw []byte) ([]byte, error) {
	var msg map[string]any
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, err
	}

	delete(msg, "id")
	return json.Marshal(msg)
}