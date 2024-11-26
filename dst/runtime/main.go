package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const (
	modulePathFlagName = "module"
)

var rootCmd = &cobra.Command{
	Use: "runtime --module=<path> [module_args]",
	RunE: func(cmd *cobra.Command, _ []string) error {
		modulePath, err := cmd.Flags().GetString(modulePathFlagName)
		if err != nil {
			return err
		}
		strippedArgs := make([]string, 0, len(os.Args))
		for _, arg := range os.Args {
			if strings.HasPrefix(arg, "--"+modulePathFlagName+"=") {
				continue
			}
			strippedArgs = append(strippedArgs, arg)
		}
		os.Args = strippedArgs
		return run(modulePath)
	},
}

func init() {
	rootCmd.Flags().String(modulePathFlagName, "", "path to the module (.wasm file) to run")
	if err := rootCmd.MarkFlagRequired(modulePathFlagName); err != nil {
		panic(err)
	}
}

// main is the entry point to run dst_test.go with a wazero runtime.
func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(fmt.Errorf("error running wasm: %w", err))
		os.Exit(1)
	}
}
