package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"tidbcloud-insight/internal/auth"
	"tidbcloud-insight/internal/cache"
	"tidbcloud-insight/internal/client"
	"tidbcloud-insight/internal/config"

	"github.com/spf13/cobra"
)

func toAPIBizType(bizTypeKey string) string {
	switch bizTypeKey {
	case "dedicated":
		return "tidbcloud"
	case "premium":
		return "prod-tidbcloud-nextgen"
	default:
		return bizTypeKey
	}
}

func NewClustersCmd(c *cache.Cache) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clusters [dedicated|premium]",
		Short: "List clusters by business type",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			bizTypeKey := args[0]
			if bizTypeKey != "dedicated" && bizTypeKey != "premium" {
				fmt.Fprintf(os.Stderr, "Invalid bizType: %s. Must be 'dedicated' or 'premium'\n", bizTypeKey)
				os.Exit(1)
			}
			apiBizType := toAPIBizType(bizTypeKey)

			vendorRegions := []client.VendorRegion{
				{Vendor: "aws", Region: "us-west-2"},
				{Vendor: "aws", Region: "us-east-1"},
				{Vendor: "aws", Region: "us-east-2"},
				{Vendor: "aws", Region: "eu-central-1"},
				{Vendor: "aws", Region: "eu-west-1"},
				{Vendor: "aws", Region: "eu-west-2"},
				{Vendor: "aws", Region: "ap-southeast-1"},
				{Vendor: "aws", Region: "ap-southeast-2"},
				{Vendor: "aws", Region: "ap-northeast-1"},
				{Vendor: "aws", Region: "ap-northeast-2"},
				{Vendor: "aws", Region: "ap-south-1"},
				{Vendor: "gcp", Region: "us-west1"},
				{Vendor: "gcp", Region: "us-east1"},
				{Vendor: "gcp", Region: "europe-west1"},
				{Vendor: "gcp", Region: "asia-east1"},
				{Vendor: "gcp", Region: "asia-southeast1"},
				{Vendor: "alicloud", Region: "ap-southeast-1"},
				{Vendor: "alicloud", Region: "ap-southeast-2"},
				{Vendor: "alicloud", Region: "ap-northeast-1"},
				{Vendor: "alicloud", Region: "eu-central-1"},
				{Vendor: "alicloud", Region: "us-east-1"},
			}

			cl := client.NewClient(c)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			allClusters, err := cl.ListClustersConcurrent(ctx, vendorRegions, apiBizType, 500, func(vendor, region string, count int) {
				if count > 0 {
					fmt.Printf("  %s/%s: %d\n", vendor, region, count)
				}
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error listing clusters: %v\n", err)
				os.Exit(1)
			}

			if len(allClusters) == 0 {
				fmt.Println("\nNo clusters found.")
				return
			}

			seen := make(map[string]bool)
			var uniqueClusters []client.Cluster
			for _, cluster := range allClusters {
				if cluster.ApplicationID != "" && !seen[cluster.ApplicationID] {
					seen[cluster.ApplicationID] = true
					uniqueClusters = append(uniqueClusters, cluster)
				}
			}

			fmt.Printf("\nTotal: %d clusters\n\n", len(uniqueClusters))
			for _, cluster := range uniqueClusters {
				fmt.Printf("%s  %s  (%s/%s)\n", cluster.ApplicationID, cluster.GetDisplayName(), cluster.Vendor, cluster.Region)
			}

			cfg := config.Get()
			var metaDir string
			if cfg != nil && cfg.Meta != "" {
				metaDir = cfg.Meta
			} else {
				metaDir = "./meta"
			}

			outputDir := filepath.Join(metaDir, bizTypeKey)
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
				return
			}

			outputFile := filepath.Join(outputDir, "clusters.txt")
			var lines []string
			for _, cluster := range uniqueClusters {
				lines = append(lines, fmt.Sprintf("%s  %s  (%s/%s)", cluster.ApplicationID, cluster.GetDisplayName(), cluster.Vendor, cluster.Region))
			}
			if err := os.WriteFile(outputFile, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing to %s: %v\n", outputFile, err)
				return
			}
			fmt.Printf("\nSaved to %s\n", outputFile)
		},
	}

	return cmd
}

func NewQueryCmd(c *cache.Cache) *cobra.Command {
	var clusterID string
	var metrics string
	var vendor string
	var region string
	var bizType string
	var projectID string
	var start int
	var end int
	var step int
	var force bool

	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query cluster metrics",
		Run: func(cmd *cobra.Command, args []string) {
			if clusterID == "" || metrics == "" {
				fmt.Fprintln(os.Stderr, "clusterID and metrics are required")
				os.Exit(1)
			}

			metricList := strings.Split(metrics, ",")
			for i, m := range metricList {
				metricList[i] = strings.TrimSpace(m)
			}

			if vendor == "" {
				vendor = "aws"
			}
			if region == "" {
				region = "us-west-2"
			}
			if bizType == "" {
				bizType = "dedicated"
			}
			apiBizType := toAPIBizType(bizType)
			if step == 0 {
				step = 60
			}

			cacheKey := c.GetQueryCacheKey(clusterID, metricList, start, end, step)

			if !force {
				cached, err := c.GetCachedMetrics(cacheKey)
				if err == nil && cached != nil {
					index := c.GetIndex()
					cacheID := index.Queries[cacheKey]
					fmt.Println(cacheID)
					return
				}
			}
			_, _ = auth.GetManager().GetToken()

			ctx := context.Background()
			cl := client.NewClient(c)
			dsURL, err := cl.GetDsURL(ctx, clusterID, vendor, region, apiBizType, projectID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error getting datasource URL: %v\n", err)
				os.Exit(1)
			}

			metricsData := make(map[string]map[string]interface{})
			for _, metric := range metricList {
				result, err := cl.QueryMetric(ctx, dsURL, metric, start, end, step)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error querying metric %s: %v\n", metric, err)
					continue
				}
				metricsData[metric] = result
			}

			cacheID, err := c.SaveMetricsCache(cacheKey, metricsData)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error saving cache: %v\n", err)
				os.Exit(1)
			}

			fmt.Println(cacheID)
		},
	}

	cmd.Flags().StringVar(&clusterID, "clusterID", "", "Cluster ID (applicationID)")
	cmd.Flags().StringVar(&metrics, "metrics", "", "Metric names to query (comma-separated)")
	cmd.Flags().StringVar(&vendor, "vendor", "", "Cloud vendor (aws, gcp, alicloud)")
	cmd.Flags().StringVar(&region, "region", "", "Cloud region")
	cmd.Flags().StringVar(&bizType, "bizType", "", "Business type (dedicated, premium)")
	cmd.Flags().StringVar(&projectID, "projectID", "", "Project ID (for k8s-infra)")
	cmd.Flags().IntVar(&start, "start", 0, "Start timestamp (for query_range)")
	cmd.Flags().IntVar(&end, "end", 0, "End timestamp (for query_range)")
	cmd.Flags().IntVar(&step, "step", 60, "Query step in seconds")
	cmd.Flags().BoolVar(&force, "force", false, "Force refresh cache")

	cmd.MarkFlagRequired("clusterID")
	cmd.MarkFlagRequired("metrics")

	cmd.AddCommand(NewCacheCmd(c))

	return cmd
}

func parseDuration(s string) int {
	s = strings.TrimSpace(strings.ToLower(s))
	if strings.HasSuffix(s, "h") {
		hours := 0
		fmt.Sscanf(s, "%dh", &hours)
		return hours * 3600
	} else if strings.HasSuffix(s, "d") {
		days := 0
		fmt.Sscanf(s, "%dd", &days)
		return days * 86400
	}
	return 48 * 3600
}
