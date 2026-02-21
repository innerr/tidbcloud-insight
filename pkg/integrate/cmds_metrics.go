package integrate

import (
	"fmt"

	impl "tidbcloud-insight/pkg/integrate/cmds_impl"

	"github.com/innerr/ticat/pkg/core/model"
)

func MetricsFetchCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	clientParams := getClientParams(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	clusterID := env.GetRaw(EnvKeyClusterID)

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	cacheID, err := impl.MetricsFetch(cacheDir, metaDir, clientParams, authMgr,
		clusterID, tr.StartUnix(), tr.EndUnix())
	if err != nil {
		return currCmdIdx, err
	}
	fmt.Println(cacheID)
	return currCmdIdx, nil
}

func MetricsFetchRandom(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	clientParams := getClientParams(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	cacheID, err := impl.MetricsFetchRandom(cacheDir, metaDir, clientParams, authMgr,
		tr.StartUnix(), tr.EndUnix())
	if err != nil {
		return currCmdIdx, err
	}
	fmt.Println(cacheID)
	return currCmdIdx, nil
}

func MetricsFetchAll(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	clientParams := getClientParams(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	err = impl.MetricsFetchAll(cacheDir, metaDir, clientParams, authMgr,
		tr.StartUnix(), tr.EndUnix())
	return currCmdIdx, err
}

func MetricsCacheListCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	clusterID := env.GetRaw(EnvKeyClusterID)
	metricName := env.GetRaw(EnvKeyMetrics)

	if err := impl.MetricsCacheList(cacheDir, clusterID, metricName); err != nil {
		return currCmdIdx, err
	}
	return currCmdIdx, nil
}

func MetricsCacheClearCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)

	if err := impl.MetricsCacheClear(cacheDir); err != nil {
		return currCmdIdx, err
	}
	return currCmdIdx, nil
}

func MetricsCacheClearClusterCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	clusterID := env.GetRaw(EnvKeyClusterID)
	if clusterID == "" {
		return currCmdIdx, fmt.Errorf("cluster-id is required")
	}

	if err := impl.MetricsCacheClearCluster(cacheDir, clusterID); err != nil {
		return currCmdIdx, err
	}
	return currCmdIdx, nil
}
