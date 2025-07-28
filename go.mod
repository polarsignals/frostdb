module github.com/polarsignals/frostdb

go 1.24.1

require (
	github.com/RoaringBitmap/roaring v1.9.4
	github.com/apache/arrow-go/v18 v18.2.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/cockroachdb/datadriven v1.0.2
	github.com/dgryski/go-metro v0.0.0-20250106013310-edb8663e5e33
	github.com/dustin/go-humanize v1.0.1
	github.com/go-kit/log v0.2.1
	github.com/google/uuid v1.6.0
	github.com/oklog/ulid v1.3.1
	github.com/oklog/ulid/v2 v2.1.0
	github.com/parquet-go/parquet-go v0.24.0
	github.com/pingcap/tidb/parser v0.0.0-20231013125129-93a834a6bf8d
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10
	github.com/polarsignals/iceberg-go v0.0.0-20240502213135-2ee70b71e76b
	github.com/polarsignals/wal v0.0.0-20240619104840-9da940027f9c
	github.com/prometheus/client_golang v1.20.5
	github.com/spf13/cobra v1.8.0
	github.com/stretchr/testify v1.10.0
	github.com/tetratelabs/wazero v1.7.3
	github.com/thanos-io/objstore v0.0.0-20240818203309-0363dadfdfb1
	go.opentelemetry.io/otel v1.37.0
	go.opentelemetry.io/otel/trace v1.37.0
	go.uber.org/goleak v1.3.0
	golang.org/x/exp v0.0.0-20250128182459-e0ece0dbea4c
	golang.org/x/sync v0.11.0
	google.golang.org/grpc v1.71.0
	google.golang.org/protobuf v1.36.5
)

require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/benbjohnson/immutable v0.4.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/coreos/etcd v3.3.27+incompatible // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20220810130054-c7d1c02cb6cf // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/efficientgo/core v1.0.0-rc.2 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/hamba/avro/v2 v2.28.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63 // indirect
	github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c // indirect
	github.com/pingcap/log v1.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.25.0 // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/tools v0.30.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Avro has a regression in parsing map[string]any in v2.20.0 and higher. Issue to track this regression: https://github.com/hamba/avro/issues/386
replace github.com/hamba/avro/v2 => github.com/hamba/avro/v2 v2.19.0
