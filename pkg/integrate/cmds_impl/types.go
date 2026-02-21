package impl

import (
	"time"

	"tidbcloud-insight/pkg/auth"
	"tidbcloud-insight/pkg/client"
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
