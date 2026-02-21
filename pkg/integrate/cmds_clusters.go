package integrate

import (
	"time"

	impl "tidbcloud-insight/pkg/integrate/cmds_impl"

	"github.com/innerr/ticat/pkg/core/model"
)

func ClustersDedicatedFetch(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)

	timeoutStr := env.GetRaw(EnvKeyClustersFetchTimeout)
	if timeoutStr == "" {
		timeoutStr = "60s"
	}
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		timeout = 60 * time.Second
	}

	pageSize := env.GetInt(EnvKeyClustersPageSize)
	if pageSize <= 0 {
		pageSize = 500
	}

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	outputFile := impl.RawClusters(cacheDir, metaDir, "dedicated", timeout, pageSize, authMgr)
	if outputFile != "" {
		env.GetLayer(model.EnvLayerSession).Set(EnvKeyClustersListFile, outputFile)
	}
	return currCmdIdx, nil
}

func ClustersPremiumFetch(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)

	timeoutStr := env.GetRaw(EnvKeyClustersFetchTimeout)
	if timeoutStr == "" {
		timeoutStr = "60s"
	}
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		timeout = 60 * time.Second
	}

	pageSize := env.GetInt(EnvKeyClustersPageSize)
	if pageSize <= 0 {
		pageSize = 500
	}

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	outputFile := impl.RawClusters(cacheDir, metaDir, "premium", timeout, pageSize, authMgr)
	if outputFile != "" {
		env.GetLayer(model.EnvLayerSession).Set(EnvKeyClustersListFile, outputFile)
	}
	return currCmdIdx, nil
}
