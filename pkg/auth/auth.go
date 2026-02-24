package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"tidbcloud-insight/pkg/logger"
)

type Manager struct {
	mu sync.RWMutex

	clientID     string
	clientSecret string
	tokenURL     string
	audience     string
	cachePath    string

	accessToken string
	expiry      time.Time

	refreshing bool
	refreshCh  chan struct{}

	lastTokenSource string

	bgCtx    context.Context
	bgCancel context.CancelFunc
	bgWg     sync.WaitGroup
}

func NewManager(clientID, clientSecret, tokenURL, audience, cachePath string) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		clientID:     clientID,
		clientSecret: clientSecret,
		tokenURL:     tokenURL,
		audience:     audience,
		cachePath:    cachePath,
		refreshCh:    make(chan struct{}, 1),
		bgCtx:        ctx,
		bgCancel:     cancel,
	}
	m.loadFromCache()
	return m
}

type tokenCacheData struct {
	AccessToken string    `json:"access_token"`
	Expiry      time.Time `json:"expiry"`
}

func (m *Manager) loadFromCache() {
	if m.cachePath == "" {
		return
	}
	data, err := os.ReadFile(m.cachePath)
	if err != nil {
		return
	}

	var cache tokenCacheData
	if err := json.Unmarshal(data, &cache); err != nil {
		return
	}

	if cache.AccessToken != "" && time.Now().Before(cache.Expiry) {
		m.mu.Lock()
		m.accessToken = cache.AccessToken
		m.expiry = cache.Expiry
		m.lastTokenSource = "cache"
		m.mu.Unlock()
	}
}

func (m *Manager) saveToCache() error {
	if m.cachePath == "" {
		return nil
	}
	m.mu.RLock()
	cache := tokenCacheData{
		AccessToken: m.accessToken,
		Expiry:      m.expiry,
	}
	m.mu.RUnlock()

	cacheDir := filepath.Dir(m.cachePath)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(m.cachePath, data, 0600)
}

func (m *Manager) fetchNewToken() error {
	if m.clientID == "" {
		return fmt.Errorf("auth info is missing in env: tidbcloud-insight.auth.client-id, tidbcloud-insight.auth.client-secret")
	}

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", m.clientID)
	data.Set("client_secret", m.clientSecret)
	data.Set("audience", m.audience)

	tokenURL := m.tokenURL
	if tokenURL == "" {
		tokenURL = "https://tidb-soc2.us.auth0.com/oauth/token"
	}

	resp, err := http.PostForm(tokenURL, data)
	if err != nil {
		return fmt.Errorf("failed to get OAuth token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OAuth request failed with status %d: %s (client_id=%s, audience=%s)", resp.StatusCode, string(body), m.clientID, m.audience)
	}

	var result struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode OAuth response: %w", err)
	}

	m.mu.Lock()
	m.accessToken = result.AccessToken
	m.expiry = time.Now().Add(time.Duration(result.ExpiresIn-60) * time.Second)
	m.refreshing = false
	m.lastTokenSource = "remote"
	m.mu.Unlock()

	_ = m.saveToCache()

	return nil
}

func (m *Manager) refreshToken() error {
	m.mu.Lock()
	if m.refreshing {
		m.mu.Unlock()
		<-m.refreshCh
		m.mu.RLock()
		valid := time.Now().Before(m.expiry)
		m.mu.RUnlock()
		if valid {
			return nil
		}
		m.mu.Lock()
	}
	m.refreshing = true
	m.mu.Unlock()

	err := m.fetchNewToken()

	m.mu.RLock()
	select {
	case m.refreshCh <- struct{}{}:
	default:
	}
	m.mu.RUnlock()

	return err
}

func (m *Manager) GetToken() (string, error) {
	m.mu.RLock()
	if m.accessToken != "" && time.Now().Before(m.expiry) {
		token := m.accessToken
		m.mu.RUnlock()
		return token, nil
	}
	m.mu.RUnlock()

	token, err := m.getTokenWithRefresh()
	if err != nil {
		return "", err
	}
	logger.Infof("[Auth] token acquired from %s", m.GetTokenSource())
	return token, nil
}

func (m *Manager) GetTokenSource() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.lastTokenSource == "" {
		return "unknown"
	}
	return m.lastTokenSource
}

func (m *Manager) getTokenWithRefresh() (string, error) {
	if err := m.refreshToken(); err != nil {
		return "", err
	}

	m.mu.RLock()
	token := m.accessToken
	m.mu.RUnlock()

	return token, nil
}

func (m *Manager) InvalidateToken() {
	m.mu.Lock()
	m.expiry = time.Time{}
	m.mu.Unlock()
}

func (m *Manager) IsTokenValid() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.accessToken != "" && time.Now().Before(m.expiry)
}

func (m *Manager) StartBackgroundRefresh() {
	m.bgWg.Add(1)
	go m.backgroundRefreshLoop()
}

func (m *Manager) Stop() {
	m.bgCancel()
	m.bgWg.Wait()
}

func (m *Manager) backgroundRefreshLoop() {
	defer m.bgWg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.bgCtx.Done():
			return
		case <-ticker.C:
			m.checkAndRefreshToken()
		}
	}
}

func (m *Manager) checkAndRefreshToken() {
	m.mu.RLock()
	expiry := m.expiry
	m.mu.RUnlock()

	if expiry.IsZero() {
		return
	}

	timeUntilExpiry := time.Until(expiry)
	refreshThreshold := 5 * time.Minute

	if timeUntilExpiry < refreshThreshold {
		logger.Infof("[Auth] token expiring in %v, refreshing proactively", timeUntilExpiry)
		_, _ = m.getTokenWithRefresh()
	}
}

func (m *Manager) GetExpiry() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.expiry
}

func (m *Manager) GetTimeUntilExpiry() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.expiry.IsZero() {
		return 0
	}
	return time.Until(m.expiry)
}
