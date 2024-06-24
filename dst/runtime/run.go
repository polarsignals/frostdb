package main

import (
	"context"
	"fmt"
	"os"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/experimental/sysfs"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/polarsignals/frostdb/dst/vfs"
)

const (
	randSeedEnvVar      = "GORANDSEED"
	compilationCacheDir = "/tmp"
)

func run(modulePath string) error {
	randSeed := os.Getenv(randSeedEnvVar)
	if randSeed == "" {
		return fmt.Errorf("%s is not set", randSeedEnvVar)
	}

	cc, err := wazero.NewCompilationCacheWithDir(compilationCacheDir)
	if err != nil {
		return fmt.Errorf("creating compilation cache: %w", err)
	}

	runtimeConfig := wazero.NewRuntimeConfig().
		// Enable debug info for better stack traces.
		WithDebugInfoEnabled(true).
		// Cache compilations to speed up subsequent runs.
		WithCompilationCache(cc)

	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)
	defer r.Close(ctx)

	config := wazero.NewModuleConfig().
		WithEnv(randSeedEnvVar, randSeed).
		WithStdin(os.Stdin).
		WithStdout(os.Stdout).
		WithStderr(os.Stderr).
		// Mount filesystem. This is taken from wazero's CLI implementation.
		WithFSConfig(wazero.NewFSConfig().(sysfs.FSConfig).WithSysFSMount(vfs.New("/"), "/")).
		// All these time-related configuration options are to allow the module
		// to access "real" time on the host. We could use this as a source of
		// determinisme, but we currently compile the module with -faketime
		// which allows us to virtually speed up time with sleeping goroutines.
		// We could eventually revisit this, but this is fine for now.
		WithSysNanosleep().
		WithSysNanotime().
		WithSysWalltime().
		WithArgs(os.Args...)

	vfs.MustInstantiate(ctx, r)

	moduleBytes, err := os.ReadFile(modulePath)
	if err != nil {
		return fmt.Errorf("reading module: %w", err)
	}

	compiledModule, err := r.CompileModule(ctx, moduleBytes)
	if err != nil {
		return fmt.Errorf("compiling module: %w", err)
	}

	wasi_snapshot_preview1.MustInstantiate(ctx, r)
	if _, err := r.InstantiateModule(ctx, compiledModule, config); err != nil {
		return fmt.Errorf("instantiating module: %w", err)
	}
	return nil
}
