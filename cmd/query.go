/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dajmeister/ddb/app"
)

// queryCmd represents the query command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "query table",
	Long:  `Query a dynamodb table for zero or more Items.`,
	Args:  cobra.RangeArgs(1, 2),
	RunE:  runQuery,
}

func runQuery(cmd *cobra.Command, args []string) error {
	tableName := viper.GetString("table")
	logger.Debug(fmt.Sprintf("describing table %s", tableName))
	keys, err := app.GetKeys(client, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table keys: %w", err)
	}
	partitionKey := keys[0] // partition key
	partitionKeyValue, err := app.MarshalArgument(args[0], partitionKey.AttributeType)
	if err != nil {
		return fmt.Errorf("failed to marshal argument 1 with value %s to type %s [%w]", partitionKeyValue, partitionKey.AttributeType, err)
	}
	keyCondition := expression.Key(partitionKey.Name).Equal(expression.Value(partitionKeyValue))
	if len(args) == 2 {
		sortKey := keys[1]
		sortKeyValue, err := app.MarshalArgument(args[1], sortKey.AttributeType)
		if err != nil {
			return fmt.Errorf("failed to marshal argument 2 with value %s to type %s [%w]", sortKeyValue, sortKey.AttributeType, err)
		}
		keyCondition = keyCondition.And(expression.Key(sortKey.Name).Equal(expression.Value(sortKeyValue)))
	}
	builder := expression.NewBuilder().WithKeyCondition(keyCondition)
	expr, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build query expression [%w]", err)
	}
	paginator := app.PaginateQuery(client, dynamodb.QueryInput{
		TableName:                 &tableName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	unmarshaller := app.UnmarshalItems(paginator)

	for item, err := range unmarshaller {
		if err != nil {
			return err
		}
		itemJson, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("Failed to Marshal item as json [%w]", err)
		}
		app.PrintJson(itemJson, viper.GetBool("pretty"), viper.GetBool("color"))
	}

	return nil
}

func init() {
	rootCmd.AddCommand(queryCmd)
}
