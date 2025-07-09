package internal

import (
	"context"
	"fmt"
	"iter"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var client *dynamodb.Client

type Attributes map[string]types.AttributeDefinition

type Key struct {
	Name          string
	KeyType       types.KeyType
	AttributeType types.ScalarAttributeType
}

type Item map[string]types.AttributeValue

func DynamodbClient() (*dynamodb.Client, error) {
	if client == nil {
		config, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("failed to load aws config")
		}
		client = dynamodb.NewFromConfig(config)
	}

	return client, nil

}

func GetKeys(client *dynamodb.Client, table string) ([]Key, error) {
	describe_output, err := client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: &table,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe table [%w]", err)
	}
	attributes := make(Attributes)
	for _, attribute := range describe_output.Table.AttributeDefinitions {
		attributes[*attribute.AttributeName] = attribute
	}
	var keys []Key
	for _, key := range describe_output.Table.KeySchema {
		key_name := *key.AttributeName
		keys = append(keys, Key{key_name, key.KeyType, attributes[key_name].AttributeType})
	}
	return keys, nil
}

func GetItem(client *dynamodb.Client, tableName string, keyValues Item) (map[string]any, error) {

	getOutput, err := client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key:       keyValues,
		TableName: &tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb.GetItem failed [%w]", err)
	}

	item, err := UnmarshalItem(getOutput.Item)
	if err != nil {
		return nil, fmt.Errorf("failed to Unmarshal Item [%w]", err)
	}

	return item, nil
}

func MarshalArgument(argumentValue string, attributeType types.ScalarAttributeType) (types.AttributeValue, error) {
	var value any
	switch attributeType {
	case types.ScalarAttributeTypeS:
		value = argumentValue
	case types.ScalarAttributeTypeN:
		value = attributevalue.Number(argumentValue)
	}
	attributeValue, err := attributevalue.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value for argument %s", argumentValue)
	}
	return attributeValue, nil
}

func UnmarshalItem(dynamodbItem Item) (map[string]any, error) {
	item := make(map[string]any)
	err := attributevalue.UnmarshalMap(dynamodbItem, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to Unmarshal Item [%w]", err)
	}
	return item, nil
}

func IterateQuery(client *dynamodb.Client, queryInput dynamodb.QueryInput) iter.Seq2[Item, error] {
	return func(yield func(Item, error) bool) {
		paginator := dynamodb.NewQueryPaginator(client, &queryInput)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.TODO())
			if err != nil {
				if !yield(nil, fmt.Errorf("failed to retrive page from query paginator [%w]", err)) {
					return
				}
			}
			for _, item := range page.Items {
				if !yield(item, nil) {
					return
				}
			}
		}
	}
}

func IterateScan(client *dynamodb.Client, scanInput dynamodb.ScanInput) iter.Seq2[Item, error] {
	return func(yield func(Item, error) bool) {
		paginator := dynamodb.NewScanPaginator(client, &scanInput)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.TODO())
			if err != nil {
				if !yield(nil, fmt.Errorf("failed to retrive page from scan paginator [%w]", err)) {
					return
				}
			}
			for _, item := range page.Items {
				if !yield(item, nil) {
					return
				}
			}
		}
	}
}

func UnmarshalItems(items iter.Seq2[Item, error]) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		for item, err := range items {
			if err != nil {
				if !yield(nil, err) {
					return
				}
			}
			unmarshalledItem, err := UnmarshalItem(item)
			if err != nil {
				if !yield(nil, fmt.Errorf("failed to Unmarshal Item [%w]", err)) {
					return
				}
			}
			if !yield(unmarshalledItem, nil) {
				return
			}
		}
	}
}
