package impl

import (
	"fmt"
	"time"
)

func DigRandom(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, startTS, endTS int64, bizType string, jsonOutput, local bool) error {

	return fmt.Errorf("DigRandom not implemented yet")
}

func DigWalk(cacheDir, metaDir string, cp ClientParams, maxBackoff time.Duration,
	authMgr *AuthManager, startTS, endTS int64, concurrency int) {

	fmt.Println("DigWalk not implemented yet")
}

func DigLocal(cacheDir string, startTS, endTS int64, cacheID string, jsonOutput bool) {
	fmt.Println("DigLocal not implemented yet")
}
