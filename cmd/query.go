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

type Operator string

const (
	Equal            Operator = "="
	LessThan         Operator = "<"
	LessThanEqual    Operator = "<="
	GreaterThan      Operator = ">"
	GreaterThanEqual Operator = ">="
)

type queryArgs struct {
	tableName      string
	indexName      string
	partitionValue string
	sortValue      string
	sortOperator   Operator
}

// queryCmd represents the query command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "query table",
	Long:  `Query a dynamodb table for zero or more Items.`,
	Args:  cobra.RangeArgs(2, 3),
	RunE:  runQuery,
}

func ParseArg(arg string) (string, Operator) {
	// <arg <=arg >arg >=arg
	var value string
	var found bool
	var prefix Operator
	prefixes := []Operator{LessThanEqual, GreaterThanEqual, LessThan, GreaterThan, Equal}

	for _, prefix = range prefixes {
		value, found = strings.CutPrefix(arg, string(prefix))
		if found {
			break
		}
	}

	if !found {
		prefix = Equal
		value = arg
	}

	return value, prefix
}

func ParseArgs(args []string) queryArgs {
	table, index, _ := strings.Cut(args[0], ":")

	partition := args[1]
	sort := ""
	prefix := Equal
	if len(args) == 3 {
		sort, prefix = ParseArg(args[2])
	}
	return queryArgs{
		tableName:      table,
		indexName:      index,
		partitionValue: partition,
		sortValue:      sort,
		sortOperator:   prefix,
	}
}

func runQuery(cmd *cobra.Command, raw_args []string) error {
	args := ParseArgs(raw_args)

	logger.Debug(fmt.Sprintf("describing table %s", args.tableName))
	var keys []internal.Key
	var err error
	if args.indexName != "" {
		keys, err = internal.GetIndexKeys(client, args.tableName, args.indexName)
	} else {
		keys, err = internal.GetTableKeys(client, args.tableName)
	}
	if err != nil {
		return fmt.Errorf("failed to get keys: %w", err)
	}
	partitionKey := keys[0] // partition key
	partitionKeyValue, err := internal.MarshalArgument(args.partitionValue, partitionKey.AttributeType)
	if err != nil {
		return fmt.Errorf("failed to marshal argument 1 with value %s to type %s [%w]", partitionKeyValue, partitionKey.AttributeType, err)
	}
	keyCondition := expression.Key(partitionKey.Name).Equal(expression.Value(partitionKeyValue))
	if args.sortValue != "" {
		sortKey := keys[1]
		sortKeyValue, err := internal.MarshalArgument(args.sortValue, sortKey.AttributeType)
		if err != nil {
			return fmt.Errorf("failed to marshal argument 2 with value %s to type %s [%w]", sortKeyValue, sortKey.AttributeType, err)
		}
		sortKeyExpression := expression.Key(sortKey.Name)
		sortValueExpression := expression.Value(sortKeyValue)
		var sortKeyCondition expression.KeyConditionBuilder
		switch args.sortOperator {
		case Equal:
			sortKeyCondition = sortKeyExpression.Equal(sortValueExpression)
		case LessThan:
			sortKeyCondition = sortKeyExpression.LessThan(sortValueExpression)
		case LessThanEqual:
			sortKeyCondition = sortKeyExpression.LessThanEqual(sortValueExpression)
		case GreaterThan:
			sortKeyCondition = sortKeyExpression.GreaterThan(sortValueExpression)
		case GreaterThanEqual:
			sortKeyCondition = sortKeyExpression.GreaterThanEqual(sortValueExpression)
		}
		keyCondition = keyCondition.And(sortKeyCondition)
	}
	builder := expression.NewBuilder().WithKeyCondition(keyCondition)
	expr, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build query expression [%w]", err)
	}
	queryInput := dynamodb.QueryInput{
		TableName:                 &args.tableName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	if args.indexName != "" {
		queryInput.IndexName = &args.indexName
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
