package integrate

import (
	"fmt"

	impl "tidbcloud-insight/pkg/integrate/cmds_impl"

	"github.com/innerr/ticat/pkg/core/model"
)

func DigCmd(
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

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	err = impl.Dig(cacheDir, metaDir, clientParams, maxBackoff, authMgr, fetcherConfig,
		clusterID, tr.StartUnix(), tr.EndUnix(), false)
	return currCmdIdx, err
}

func DigRandomCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	maxBackoff := getMaxBackoff(env)
	clientParams := getClientParams(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	err = impl.DigRandom(cacheDir, metaDir, clientParams, maxBackoff, authMgr,
		tr.StartUnix(), tr.EndUnix(),
		env.GetRaw(EnvKeyBizType),
		false,
		env.GetBool(EnvKeyLocal))
	return currCmdIdx, err
}

func DigWalkCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)
	metaDir := getMetaDir(env)
	maxBackoff := getMaxBackoff(env)
	clientParams := getClientParams(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	authMgr := getAuthParams(env, cacheDir).NewManager()
	authMgr.StartBackgroundRefresh()
	defer authMgr.Stop()

	impl.DigWalk(cacheDir, metaDir, clientParams, maxBackoff, authMgr,
		tr.StartUnix(), tr.EndUnix(),
		env.GetInt(EnvKeyConcurrency))
	return currCmdIdx, nil
}
