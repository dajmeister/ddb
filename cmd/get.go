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

	"github.com/dajmeister/ddb/app"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "get item",
	Long:  `Get an Item from a dynamodb table.`,
	Args:  cobra.RangeArgs(1, 2),
	RunE:  runGet,
}

func runGet(cmd *cobra.Command, args []string) error {
	tableName := viper.GetString("table")
	logger.Debug(fmt.Sprintf("describing table %s", tableName))
	keys, err := app.GetKeys(client, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table keys: %w", err)
	}

	if len(args) != len(keys) {
		return fmt.Errorf("get requires one argument per key, %d were provided. table %s has %d key(s): %v", len(args), tableName, len(keys), keys)
	}
	getKeys := make(map[string]types.AttributeValue)
	for element, arg := range args {
		key := keys[element]
		name := key.Name

		attributeValue, err := app.MarshalArgument(arg, key.AttributeType)
		if err != nil {
			return fmt.Errorf("failed to marshal argument %d with value: %s to type: %s - [%w]", element, arg, key.AttributeType, err)
		}
		getKeys[name] = attributeValue
	}
	logger.Debug("running get")
	item, err := app.GetItem(client, tableName, getKeys)
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
	app.PrintJson(itemJson, viper.GetBool("pretty"), viper.GetBool("color"))

	return nil
}

func init() {
	rootCmd.AddCommand(getCmd)
}
