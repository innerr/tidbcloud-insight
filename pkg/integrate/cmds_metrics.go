package integrate

import (
	"fmt"
	"time"

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
	metricFilter := env.GetRaw(EnvKeyMetrics)

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	fetchedCount, err := impl.MetricsFetch(cacheDir, metaDir, clientParams, authMgr,
		clusterID, tr.StartUnix(), tr.EndUnix(), metricFilter)
	if err != nil {
		return currCmdIdx, err
	}
	fmt.Printf("  Saved %d metrics to cache for cluster %s\n", fetchedCount, clusterID)
	fmt.Printf("  Time range: %s ~ %s (%s)\n", tr.Start.Format("2006-01-02 15:04:05"), tr.End.Format("2006-01-02 15:04:05"), tr.Duration().Round(time.Second))
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

	metricFilter := env.GetRaw(EnvKeyMetrics)

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	_, err = impl.MetricsFetchRandom(cacheDir, metaDir, clientParams, authMgr,
		tr.StartUnix(), tr.EndUnix(), metricFilter)
	if err != nil {
		return currCmdIdx, err
	}
	fmt.Printf("  Time range: %s ~ %s (%s)\n", tr.Start.Format("2006-01-02 15:04:05"), tr.End.Format("2006-01-02 15:04:05"), tr.Duration().Round(time.Second))
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
	metricName := env.GetRaw(EnvKeyMetrics)

	if err := impl.MetricsCacheClearCluster(cacheDir, clusterID, metricName); err != nil {
		return currCmdIdx, err
	}
	return currCmdIdx, nil
}
