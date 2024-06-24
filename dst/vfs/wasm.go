package vfs

import (
	"context"

	"github.com/tetratelabs/wazero"
)

const wasmModuleName = "vfs"

var isShutdown = false

func shutdown() {
	isShutdown = true
}

func restart() {
	isShutdown = false
}

func MustInstantiate(ctx context.Context, r wazero.Runtime) {
	if _, err := r.NewHostModuleBuilder(wasmModuleName).
		NewFunctionBuilder().WithFunc(shutdown).Export("shutdown").
		NewFunctionBuilder().WithFunc(restart).Export("restart").
		Instantiate(ctx); err != nil {
		panic(err)
	}
}
