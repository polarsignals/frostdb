//go:build wasm

package dst

//go:wasmimport vfs shutdown
func vfsShutdown()

//go:wasmimport vfs restart
func vfsRestart()
