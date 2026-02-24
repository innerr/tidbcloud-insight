package integrate

import (
	"fmt"

	impl "tidbcloud-insight/pkg/integrate/cmds_impl"

	"github.com/innerr/ticat/pkg/core/model"
)

func DigProfileCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	maxBackoff := getMaxBackoff(env)
	clientParams := getClientParams(env)
	fetcherConfig := impl.NewMetricsFetcherConfigFromEnv(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	clusterID := env.GetRaw(EnvKeyClusterID)
	if clusterID == "" {
		return currCmdIdx, fmt.Errorf("cluster-id is required")
	}

	authParams, err := getAuthParams(env, cacheDir)
	if err != nil {
		return currCmdIdx, err
	}
	authMgr := authParams.NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	err = impl.DigProfile(cacheDir, metaDir, clientParams, maxBackoff, authMgr, fetcherConfig,
		clusterID, tr.StartUnix(), tr.EndUnix(), false)
	return currCmdIdx, err
}

func DigAbnormalCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	maxBackoff := getMaxBackoff(env)
	clientParams := getClientParams(env)
	fetcherConfig := impl.NewMetricsFetcherConfigFromEnv(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	clusterID := env.GetRaw(EnvKeyClusterID)
	if clusterID == "" {
		return currCmdIdx, fmt.Errorf("cluster-id is required")
	}

	authParams, err := getAuthParams(env, cacheDir)
	if err != nil {
		return currCmdIdx, err
	}
	authMgr := authParams.NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	err = impl.DigAbnormal(cacheDir, metaDir, clientParams, maxBackoff, authMgr, fetcherConfig,
		clusterID, tr.StartUnix(), tr.EndUnix(), false)
	return currCmdIdx, err
}

func DigRandomProfileCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	maxBackoff := getMaxBackoff(env)
	clientParams := getClientParams(env)
	fetcherConfig := impl.NewMetricsFetcherConfigFromEnv(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	authParams, err := getAuthParams(env, cacheDir)
	if err != nil {
		return currCmdIdx, err
	}
	authMgr := authParams.NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	err = impl.DigRandomProfile(cacheDir, metaDir, clientParams, maxBackoff, authMgr, fetcherConfig,
		tr.StartUnix(), tr.EndUnix(), false)
	return currCmdIdx, err
}

func DigRandomAbnormalCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	maxBackoff := getMaxBackoff(env)
	clientParams := getClientParams(env)
	fetcherConfig := impl.NewMetricsFetcherConfigFromEnv(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	authParams, err := getAuthParams(env, cacheDir)
	if err != nil {
		return currCmdIdx, err
	}
	authMgr := authParams.NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	err = impl.DigRandomAbnormal(cacheDir, metaDir, clientParams, maxBackoff, authMgr, fetcherConfig,
		tr.StartUnix(), tr.EndUnix(), false)
	return currCmdIdx, err
}

func DigWalkProfileCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	maxBackoff := getMaxBackoff(env)
	clientParams := getClientParams(env)
	fetcherConfig := impl.NewMetricsFetcherConfigFromEnv(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	authParams, err := getAuthParams(env, cacheDir)
	if err != nil {
		return currCmdIdx, err
	}
	authMgr := authParams.NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	err = impl.DigWalkProfile(cacheDir, metaDir, clientParams, maxBackoff, authMgr, fetcherConfig,
		tr.StartUnix(), tr.EndUnix())
	return currCmdIdx, err
}

func DigWalkAbnormalCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	maxBackoff := getMaxBackoff(env)
	clientParams := getClientParams(env)
	fetcherConfig := impl.NewMetricsFetcherConfigFromEnv(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	authParams, err := getAuthParams(env, cacheDir)
	if err != nil {
		return currCmdIdx, err
	}
	authMgr := authParams.NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	err = impl.DigWalkAbnormal(cacheDir, metaDir, clientParams, maxBackoff, authMgr, fetcherConfig,
		tr.StartUnix(), tr.EndUnix())
	return currCmdIdx, err
}
