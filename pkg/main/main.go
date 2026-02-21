package main

import (
	"os"

	"tidbcloud-insight/pkg/integrate"

	"github.com/innerr/ticat/pkg/ticat"
)

const (
	GitHash   string = ""
	GitDirty  string = ""
	DirtyHash string = ""
	BuildTime string = ""
)

func main() {
	tc := ticat.NewTiCat()
	err := integrate.Integrate(tc)
	if err != nil {
		panic(err)
	}
	tc.RunCli(os.Args[1:]...)
}
