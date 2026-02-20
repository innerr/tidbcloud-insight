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

	"tidbcloud-insight/internal/config"
	"tidbcloud-insight/internal/logger"
)

type Manager struct {
	mu sync.RWMutex

	accessToken string
	expiry      time.Time

	cfg       *config.Config
	cachePath string

	refreshing bool
	refreshCh  chan struct{}

	lastTokenSource string

	bgCtx    context.Context
	bgCancel context.CancelFunc
	bgWg     sync.WaitGroup
}

var (
	defaultManager *Manager
	managerOnce    sync.Once
)

func GetManager() *Manager {
	managerOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defaultManager = &Manager{
			cfg:       config.Get(),
			cachePath: getCacheFilePath(),
			refreshCh: make(chan struct{}, 1),
			bgCtx:     ctx,
			bgCancel:  cancel,
		}
		defaultManager.loadFromCache()
	})
	return defaultManager
}

func getCacheFilePath() string {
	cfg := config.Get()
	if cfg != nil && cfg.Cache != "" {
		return filepath.Join(cfg.Cache, "auth.json")
	}
	return filepath.Join(".", "cache", "auth.json")
}

type tokenCacheData struct {
	AccessToken string    `json:"access_token"`
	Expiry      time.Time `json:"expiry"`
}

func (m *Manager) loadFromCache() {
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
		logger.Infof("[Auth] loaded token from cache, expires at %v", cache.Expiry.Format(time.RFC3339))
	}
}

func (m *Manager) saveToCache() error {
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
	if m.cfg == nil {
		return fmt.Errorf("config not loaded")
	}

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", m.cfg.Auth.ClientID)
	data.Set("client_secret", m.cfg.Auth.ClientSecret)
	data.Set("audience", m.cfg.Auth.Audience)

	tokenURL := m.cfg.Auth.TokenURL
	if tokenURL == "" {
		tokenURL = "https://tidb-soc2.us.auth0.com/oauth/token"
	}

	resp, err := http.PostForm(tokenURL, data)
	if err != nil {
		return fmt.Errorf("failed to get OAuth token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OAuth request failed with status %d: %s (client_id=%s, audience=%s)", resp.StatusCode, string(body), m.cfg.Auth.ClientID, m.cfg.Auth.Audience)
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

	if err := m.saveToCache(); err != nil {
		logger.Warnf("[Auth] failed to save token to cache: %v", err)
	} else {
		logger.Infof("[Auth] token saved to cache, expires at %v", m.expiry.Format(time.RFC3339))
	}

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

	return m.getTokenWithRefresh()
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

func GetOAuthToken() (string, error) {
	return GetManager().GetToken()
}

func (m *Manager) StartBackgroundRefresh() {
	m.bgWg.Add(1)
	go m.backgroundRefreshLoop()
	logger.Infof("[Auth] background refresh started")
}

func (m *Manager) Stop() {
	m.bgCancel()
	m.bgWg.Wait()
	logger.Infof("[Auth] background refresh stopped")
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
