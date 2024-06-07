# DST

This folder is where FrostDB's deterministic simulation test workload (see `dst_test.go`) and runtime are stored.

## Architecture

Read the [blog post on DST in FrostDB](https://www.polarsignals.com/blog/posts/2024/05/28/mostly-dst-in-go) for a general background. In short, the deterministic simulation test is run as a workload against frostdb in `TestDST`. Determinism is achieved by using a custom go runtime to compile the test and subsequently using `wazero` with a special config (see the `runtime` folder) seeded with the `GORANDSEED` variable.

## How to run

### Prerequisites

- [github.com/polarsignals/go](github.com/polarsignals/go): download the repository and run `./make.bash` in the `src` directory to build the custom go runtime.

### Compile the test to a WASM module

Compile the test with the custom go runtime. First set the `GOROOT` environment variable to the path of the custom go runtime and then compile (without running) the test binary to wasm.

```bash
export GOROOT=/path/to/polarsignals/go
GOOS=wasip1 GOARCH=wasm $GOROOT/bin/go test -tags=faketime -c -o dst/dst.wasm ./dst
```

### Run the test using the pre-configured wazero runtime

See `runtime/run.go` for the runtime configuration. The `GORANDSEED` environment variable is used to seed the custom go runtime so must be provided for tests. Note the `-test.v` flag is used to run the test in verbose mode. Any go testing flags can be added here at your convenience.

```bash
go build ./dst/runtime
GORANDSEED=<seed> ./runtime --module=./dst/dst.wasm -test.v
```
