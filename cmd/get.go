/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dajmeister/ddb/internal"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "get item",
	Long:  `Get an Item from a dynamodb table.`,
	Args:  cobra.RangeArgs(2, 3),
	RunE:  runGet,
}

func runGet(cmd *cobra.Command, args []string) error {
	tableName, partitionValue := args[0], args[1]
	sortValue := ""
	if len(args) == 3 {
		sortValue = args[2]
	}
	logger.Debug(fmt.Sprintf("describing table %s", tableName))
	keys, err := internal.GetTableKeys(client, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table keys: %w", err)
	}
	getKeys := make(map[string]types.AttributeValue)
	partitionKey := keys[0] // partition key
	partitionKeyValue, err := internal.MarshalArgument(partitionValue, partitionKey.AttributeType)
	if err != nil {
		return fmt.Errorf("failed to marshal argument 1 with value %s to type %s [%w]", partitionKeyValue, partitionKey.AttributeType, err)
	}
	getKeys[partitionKey.Name] = partitionKeyValue
	if len(keys) == 2 {
		if len(args) != 3 {
			return fmt.Errorf("get requires one argument per key, %d were provided. table %s has keys: %v", len(args)-1, tableName, keys)
		}
		sortKey := keys[1]
		sortKeyValue, err := internal.MarshalArgument(sortValue, sortKey.AttributeType)
		if err != nil {
			return fmt.Errorf("failed to marshal argument 2 with value %s to type %s [%w]", sortKeyValue, sortKey.AttributeType, err)
		}
		getKeys[sortKey.Name] = sortKeyValue
	}
	logger.Debug("running get")
	item, err := internal.GetItem(client, tableName, getKeys)
	if err != nil {
		return fmt.Errorf("failed to get item: %w", err)
	}

	if len(item) == 0 {
		return nil
	}
	itemJson, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal item as json: %w", err)
	}
	internal.PrintJson(itemJson, viper.GetBool("pretty"), viper.GetBool("color"))

	return nil
}

func init() {
	rootCmd.AddCommand(getCmd)
}
