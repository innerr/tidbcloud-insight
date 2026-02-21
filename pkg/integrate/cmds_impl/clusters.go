package impl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"tidbcloud-insight/pkg/auth"
	"tidbcloud-insight/pkg/client"
	cache "tidbcloud-insight/pkg/local_cache"
)

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

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

func RawClusters(cacheDir, metaDir, bizType string, timeout time.Duration, pageSize int, authMgr *auth.Manager) string {
	c, err := cache.NewCache(cacheDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing cache: %v\n", err)
		return ""
	}

	apiBizType := toAPIBizType(bizType)

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

	cl := client.NewClientSerialWithAuth(c, authMgr)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	allClusters, err := cl.ListClusters(ctx, vendorRegions, apiBizType, pageSize, func(vendor, region string, count int) {
		if count > 0 {
			fmt.Printf("  %s/%s: %d\n", vendor, region, count)
		}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing clusters: %v\n", err)
		return ""
	}

	if len(allClusters) == 0 {
		fmt.Println("\nNo clusters found.")
		return ""
	}

	seen := make(map[string]bool)
	var uniqueClusters []client.Cluster
	for _, cluster := range allClusters {
		if cluster.ApplicationID != "" && !seen[cluster.ApplicationID] && isAllDigits(cluster.ApplicationID) {
			seen[cluster.ApplicationID] = true
			uniqueClusters = append(uniqueClusters, cluster)
		}
	}

	fmt.Printf("\nTotal: %d clusters\n\n", len(uniqueClusters))
	for _, cluster := range uniqueClusters {
		fmt.Printf("%s  %s  (%s/%s)\n", cluster.ApplicationID, cluster.GetDisplayName(), cluster.Vendor, cluster.Region)
	}

	outputDir := filepath.Join(metaDir, bizType)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
		return ""
	}

	outputFile := filepath.Join(outputDir, "clusters.txt")
	tmpFile := outputFile + ".tmp"

	var lines []string
	for _, cluster := range uniqueClusters {
		lines = append(lines, fmt.Sprintf("%s  %s  (%s/%s)", cluster.ApplicationID, cluster.GetDisplayName(), cluster.Vendor, cluster.Region))
	}

	if err := os.WriteFile(tmpFile, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing to %s: %v\n", tmpFile, err)
		return ""
	}

	if err := os.Rename(tmpFile, outputFile); err != nil {
		fmt.Fprintf(os.Stderr, "Error renaming %s to %s: %v\n", tmpFile, outputFile, err)
		_ = os.Remove(tmpFile)
		return ""
	}

	fmt.Printf("\nSaved to %s\n", outputFile)
	return outputFile
}
