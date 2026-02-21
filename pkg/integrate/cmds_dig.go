package integrate

import (
	"fmt"

	impl "tidbcloud-insight/pkg/integrate/cmds_impl"

	"github.com/innerr/ticat/pkg/core/model"
)

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
		env.GetBool(EnvKeyJSON),
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

func DigLocalCmd(
	argv model.ArgVals,
	cc *model.Cli,
	env *model.Env,
	flow *model.ParsedCmds,
	currCmdIdx int) (int, error) {

	cacheDir := getCacheDir(env)

	tr, err := getTimeRangeFromEnv(env)
	if err != nil {
		return currCmdIdx, fmt.Errorf("invalid time range: %w", err)
	}

	impl.DigLocal(cacheDir, tr.StartUnix(), tr.EndUnix(),
		env.GetRaw(EnvKeyCacheID),
		env.GetBool(EnvKeyJSON))
	return currCmdIdx, nil
}
