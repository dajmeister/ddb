/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dajmeister/ddb/internal"
)

// scanCmd represents the scan command
var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: runScan,
}

func runScan(cmd *cobra.Command, args []string) error {
	tableName := viper.GetString("table")
	paginator := internal.IterateScan(client, dynamodb.ScanInput{
		TableName: &tableName,
	})

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
	rootCmd.AddCommand(scanCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// scanCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// scanCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
