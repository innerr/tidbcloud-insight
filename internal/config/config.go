package config

import (
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Auth         AuthConfig      `yaml:"auth"`
	Cache        string          `yaml:"cache"`
	Meta         string          `yaml:"meta"`
	RateLimit    RateLimitConfig `yaml:"rate_limit"`
	FetchTimeout string          `yaml:"fetch_timeout"`
}

type AuthConfig struct {
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
	TokenURL     string `yaml:"token_url"`
	Audience     string `yaml:"audience"`
}

type RateLimitConfig struct {
	MaxBackoff          string `yaml:"max_backoff"`
	DesiredConcurrency  int    `yaml:"desired_concurrency"`
	RecoveryInterval    string `yaml:"recovery_interval"`
	MinRecoveryInterval string `yaml:"min_recovery_interval"`
}

func (r RateLimitConfig) GetMaxBackoff() time.Duration {
	if r.MaxBackoff == "" {
		return 5 * time.Minute
	}
	d, err := time.ParseDuration(r.MaxBackoff)
	if err != nil {
		return 5 * time.Minute
	}
	return d
}

func (r RateLimitConfig) GetDesiredConcurrency() int {
	if r.DesiredConcurrency <= 0 {
		return 3
	}
	return r.DesiredConcurrency
}

func (r RateLimitConfig) GetRecoveryInterval() time.Duration {
	if r.RecoveryInterval == "" {
		return 30 * time.Second
	}
	d, err := time.ParseDuration(r.RecoveryInterval)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

func (r RateLimitConfig) GetMinRecoveryInterval() time.Duration {
	if r.MinRecoveryInterval == "" {
		return 10 * time.Second
	}
	d, err := time.ParseDuration(r.MinRecoveryInterval)
	if err != nil {
		return 10 * time.Second
	}
	return d
}

func (c *Config) GetFetchTimeout() time.Duration {
	if c == nil || c.FetchTimeout == "" {
		return 5 * time.Minute
	}
	d, err := time.ParseDuration(c.FetchTimeout)
	if err != nil {
		return 5 * time.Minute
	}
	return d
}

var cfg *Config

func Load(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = "tidbcloud-insight.yaml"
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, err
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}

	if c.Cache != "" && !filepath.IsAbs(c.Cache) {
		absPath, _ := filepath.Abs(c.Cache)
		c.Cache = absPath
	}

	if c.Meta != "" && !filepath.IsAbs(c.Meta) {
		absPath, _ := filepath.Abs(c.Meta)
		c.Meta = absPath
	}

	cfg = &c
	return &c, nil
}

func Get() *Config {
	return cfg
}

func SetDefault() {
	cfg = &Config{
		Auth: AuthConfig{
			TokenURL: "https://tidb-soc2.us.auth0.com/oauth/token",
			Audience: "https://tidb-soc2.us.auth0.com/api/v2/",
		},
		Cache: "./cache",
		Meta:  "./meta",
		RateLimit: RateLimitConfig{
			MaxBackoff:          "5m",
			DesiredConcurrency:  3,
			RecoveryInterval:    "30s",
			MinRecoveryInterval: "10s",
		},
	}
}

func Default() *Config {
	SetDefault()
	return cfg
}
