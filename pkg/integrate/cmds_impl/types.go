package impl

import (
	"time"

	"tidbcloud-insight/pkg/auth"
	"tidbcloud-insight/pkg/client"
)

const (
	EnvPrefix = "tidbcloud-insight."

	EnvKeyTargetChunkSizeMB           = EnvPrefix + "fetch.target-chunk-size-mb"
	EnvKeyRateLimitDesiredConcurrency = EnvPrefix + "rate-limit.desired-concurrency"
)

type AuthManager = auth.Manager

type ClientParams struct {
	FetchTimeout time.Duration
	IdleTimeout  time.Duration
	DisplayVerb  bool
}

func NewClient(cacheDir string, cp ClientParams, authMgr *auth.Manager) (*client.Client, error) {
	return client.NewClientWithAuth(cacheDir, cp.FetchTimeout, cp.IdleTimeout, cp.DisplayVerb, authMgr)
}
