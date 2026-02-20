package main

import (
	"fmt"
	"os"

	"tidbcloud-insight/internal/auth"
	"tidbcloud-insight/internal/commands"
	"tidbcloud-insight/internal/config"
	cache "tidbcloud-insight/internal/local_cache"
	"tidbcloud-insight/internal/logger"

	"github.com/spf13/cobra"
)

var cfgFile string
var verbose bool

func main() {
	rootCmd := &cobra.Command{
		Use:   "tidbcloud-insight",
		Short: "TiDB Cloud Insight Tool",
		Long: `TiDB Cloud Insight 

Examples:
  # Analyze cluster for anomalies and patterns
  tidbcloud-insight dig 1234567890 --duration 7d

  # Analyze a random cluster
  tidbcloud-insight dig random --duration 7d

  # List all dedicated clusters
  tidbcloud-insight raw clusters dedicated

  # Query cluster metrics
  tidbcloud-insight raw query --clusterID 1234567890 --metrics tidb_server_query_total

  # Manage cache
  tidbcloud-insight raw query cache --list
`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			_, err := config.Load(cfgFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
				fmt.Fprintf(os.Stderr, "Using default config.\n")
				config.SetDefault()
			}
			cfg := config.Get()
			isVerbose := verbose || (cfg != nil && cfg.Logging.Verbose)
			logger.SetVerbose(isVerbose)
			auth.GetManager().StartBackgroundRefresh()
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is ./tidbcloud-insight.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")

	c, err := cache.NewCache(getCacheDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing cache: %v\n", err)
		os.Exit(1)
	}

	rawCmd := &cobra.Command{
		Use:   "raw",
		Short: "Raw API operations",
		Long:  "Direct access to cluster listing and metric queries",
	}
	rawCmd.AddCommand(commands.NewClustersCmd(c))
	rawCmd.AddCommand(commands.NewQueryCmd(c))

	rootCmd.AddCommand(rawCmd)
	rootCmd.AddCommand(commands.NewDigCmd(c))

	rootCmd.CompletionOptions.DisableDefaultCmd = true

	err = rootCmd.Execute()
	auth.GetManager().Stop()
	if err != nil {
		os.Exit(1)
	}
}

func getCacheDir() string {
	cfg := config.Get()
	if cfg != nil && cfg.Cache != "" {
		return cfg.Cache
	}
	return "./cache"
}
