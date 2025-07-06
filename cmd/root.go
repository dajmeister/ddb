/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"golang.org/x/term"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dajmeister/ddb/app"
)

var cfgFile string
var logger *slog.Logger
var client *dynamodb.Client

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "ddb",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	SilenceUsage: true, // don't print usage if a subcommand fails
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		setupLogger()
		var err error
		client, err = app.DynamodbClient()
		if err != nil {
			logger.Error("failed to create dynamodb client")
			return err
		}
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func setupLogger() {
	log_level := slog.LevelInfo
	if viper.GetBool("verbose") {
		log_level = slog.LevelDebug
	}
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: log_level}))
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.ddb.yaml)")
	rootCmd.PersistentFlags().StringP("table", "t", "", "table name")
	rootCmd.PersistentFlags().BoolP("pretty", "p", true, "pretty print items")
	rootCmd.PersistentFlags().Bool("color", true, "don't color output")
	rootCmd.PersistentFlags().BoolP("verbose", "v", false, "verbose output")

	rootCmd.MarkPersistentFlagRequired("table")
}

func initConfig() {
	viper.BindPFlags(rootCmd.PersistentFlags())

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".ddb" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".ddb")
	}

	// if stdout isn't a tty disable pretty printing and color by default
	if !term.IsTerminal(int(os.Stdout.Fd())) {
		viper.SetDefault("pretty", false)
		viper.SetDefault("color", false)
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
