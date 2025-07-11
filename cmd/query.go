/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dajmeister/ddb/internal"
)

type queryArgs struct {
	tableName    string
	indexName    string
	partitionKey string
	sortKey      string
}

// queryCmd represents the query command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "query table",
	Long:  `Query a dynamodb table for zero or more Items.`,
	Args:  cobra.RangeArgs(2, 3),
	RunE:  runQuery,
}

func parseArgs(args []string) (string, string, string, string) {
	table, index, _ := strings.Cut(args[0], ":")

	partition := args[1]
	sort := ""
	if len(args) == 3 {
		sort = args[2]
	}
	return table, index, partition, sort
}

func runQuery(cmd *cobra.Command, args []string) error {

	tableName, indexName, partitionValue, sortValue := parseArgs(args)
	logger.Debug(fmt.Sprintf("describing table %s", tableName))
	var keys []internal.Key
	var err error
	if indexName != "" {
		keys, err = internal.GetIndexKeys(client, tableName, indexName)
	} else {
		keys, err = internal.GetTableKeys(client, tableName)
	}
	if err != nil {
		return fmt.Errorf("failed to get keys: %w", err)
	}
	partitionKey := keys[0] // partition key
	partitionKeyValue, err := internal.MarshalArgument(partitionValue, partitionKey.AttributeType)
	if err != nil {
		return fmt.Errorf("failed to marshal argument 1 with value %s to type %s [%w]", partitionKeyValue, partitionKey.AttributeType, err)
	}
	keyCondition := expression.Key(partitionKey.Name).Equal(expression.Value(partitionKeyValue))
	if sortValue != "" {
		sortKey := keys[1]
		sortKeyValue, err := internal.MarshalArgument(sortValue, sortKey.AttributeType)
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
	queryInput := dynamodb.QueryInput{
		TableName:                 &tableName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	if indexName != "" {
		queryInput.IndexName = &indexName
	}
	paginator := internal.IterateQuery(client, queryInput)

	unmarshaller := internal.UnmarshalItems(paginator)

	for item, err := range unmarshaller {
		if err != nil {
			return err
		}
		itemJson, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("Failed to Marshal item as json [%w]", err)
		}
		internal.PrintJson(itemJson, viper.GetBool("pretty"), viper.GetBool("color"))
	}

	return nil
}

func init() {
	rootCmd.AddCommand(queryCmd)
}
