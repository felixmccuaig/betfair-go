package betfair

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Authenticator struct {
	appKey   string
	username string
	password string
}

func NewAuthenticator(appKey, username, password string) *Authenticator {
	return &Authenticator{
		appKey:   appKey,
		username: username,
		password: password,
	}
}

func (a *Authenticator) Login() (string, error) {
	form := url.Values{}
	form.Set("username", a.username)
	form.Set("password", a.password)

	req, err := http.NewRequest(http.MethodPost, "https://identitysso.betfair.com/api/login", strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("create login request: %w", err)
	}

	req.Header.Set("X-Application", a.appKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("perform login request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read login response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("login failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	type loginResponse struct {
		SessionToken string `json:"sessionToken"`
		Token        string `json:"token"`
		LoginStatus  string `json:"loginStatus"`
		Status       string `json:"status"`
		StatusCode   string `json:"statusCode"`
		Error        string `json:"error"`
		ErrorDetails string `json:"errorDetails"`
	}

	var lr loginResponse
	if err := json.Unmarshal(body, &lr); err != nil {
		return "", fmt.Errorf("decode login response: %w (body=%s)", err, strings.TrimSpace(string(body)))
	}

	status := strings.ToUpper(firstNonEmpty(lr.LoginStatus, lr.Status, lr.StatusCode))
	if status != "" && status != "SUCCESS" {
		errMsg := firstNonEmpty(lr.Error, lr.ErrorDetails, strings.TrimSpace(string(body)))
		return "", fmt.Errorf("login %s: %s", status, errMsg)
	}

	token := firstNonEmpty(lr.SessionToken, lr.Token)
	if token == "" {
		for _, cookie := range resp.Cookies() {
			if strings.EqualFold(cookie.Name, "ssoid") {
				token = cookie.Value
				break
			}
		}
	}

	if token == "" {
		return "", fmt.Errorf("login response did not include a session token (body=%s)", strings.TrimSpace(string(body)))
	}

	return token, nil
}

func IsInvalidSessionError(err error) bool {
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "invalid_session_information") ||
		strings.Contains(errStr, "unrecognisedcredentials") ||
		strings.Contains(errStr, "no_session")
}