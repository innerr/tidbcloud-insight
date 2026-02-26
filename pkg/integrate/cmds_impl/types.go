package impl

import (
	"time"

	"tidbcloud-insight/pkg/auth"
	"tidbcloud-insight/pkg/client"
)

const (
	EnvPrefix = "tidbcloud-insight."

	EnvKeyTargetChunkSizeMB           = EnvPrefix + "fetch.target-chunk-size-mb"
	EnvKeyMetricsFetchStep            = EnvPrefix + "metrics-fetch-step"
	EnvKeyRateLimitDesiredConcurrency = EnvPrefix + "rate-limit.desired-concurrency"
	EnvKeyCacheMaxSizeMB              = EnvPrefix + "cache.max-size-mb"
	EnvKeyFetchMaxRetries             = EnvPrefix + "fetch.max-retries"
)

type AuthManager = auth.Manager

type ConcurrencyConfig struct {
	DesiredConcurrency  int
	RecoveryInterval    time.Duration
	MinRecoveryInterval time.Duration
}

type ClientParams struct {
	FetchTimeout time.Duration
	IdleTimeout  time.Duration
	DisplayVerb  bool
	Concurrency  ConcurrencyConfig
}

func NewClient(cacheDir string, cp ClientParams, authMgr *auth.Manager) (*client.Client, error) {
	return client.NewClientWithAuthAndConcurrencyConfig(cacheDir, cp.FetchTimeout, cp.IdleTimeout, cp.DisplayVerb,
		cp.Concurrency.DesiredConcurrency, cp.Concurrency.RecoveryInterval, cp.Concurrency.MinRecoveryInterval, authMgr)
}
