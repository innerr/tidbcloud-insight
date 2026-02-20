package commands

import (
	"fmt"
	"os"

	"tidbcloud-insight/internal/local_cache"

	"github.com/spf13/cobra"
)

func NewCacheCmd(c *cache.Cache) *cobra.Command {
	var list bool
	var clear bool

	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Manage metric cache",
		Run: func(cmd *cobra.Command, args []string) {
			if list {
				fmt.Println("Cached queries:")
				index := c.GetIndex()

				type cacheEntry struct {
					key string
					id  string
				}
				var entries []cacheEntry
				for key, id := range index.Queries {
					entries = append(entries, cacheEntry{key: key, id: id})
				}

				for _, entry := range entries {
					fmt.Printf("  [%s]\n", entry.id)
				}
			}

			if clear {
				if err := c.Clear(); err != nil {
					fmt.Fprintf(os.Stderr, "Error clearing cache: %v\n", err)
					os.Exit(1)
				}
				fmt.Println("Cache cleared")
			}

			if !list && !clear {
				cmd.Help()
			}
		},
	}

	cmd.Flags().BoolVar(&list, "list", false, "List cached queries")
	cmd.Flags().BoolVar(&clear, "clear", false, "Clear all cache")

	return cmd
}
