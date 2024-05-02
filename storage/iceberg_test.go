package storage

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/iceberg-go/catalog"
	"github.com/polarsignals/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func Test_IcebergMaintenance(t *testing.T) {
	defaultWriterOptions = []table.WriterOption{
		table.WithManifestSizeBytes(8 * 1024 * 1024), // 8MiB manifest size
		table.WithMergeSchema(),
		table.WithExpireSnapshotsOlderThan(time.Nanosecond),
		table.WithMetadataDeleteAfterCommit(),
		table.WithMetadataPreviousVersionsMax(0),
	}
	bucket := objstore.NewInMemBucket()
	iceberg, err := NewIceberg("/", catalog.NewHDFS("/", bucket), bucket)
	require.NoError(t, err)

	type Element struct {
		Name, Symbol string
		Number       int
		Mass         float64
	}

	b := &bytes.Buffer{}
	err = parquet.Write(b, []Element{
		{"Hydrogen", "H", 1, 1.00794},
		{"Helium", "He", 2, 4.002602},
		{"Lithium", "Li", 3, 6.941},
		{"Beryllium", "Be", 4, 9.012182},
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, iceberg.Upload(ctx, "db/table/ulid/data.parquet", b))

	var modtime time.Time
	var deleted string
	require.NoError(t, bucket.Iter(ctx, "", func(name string) error {
		if strings.HasSuffix(name, ".parquet") { // Found the data file
			deleted = name
			id, err := ulid.Parse(filepath.Base(strings.TrimSuffix(name, ".parquet")))
			require.NoError(t, err)
			modtime = ulid.Time(id.Time())
		}
		return nil
	}, objstore.WithRecursiveIter))

	b.Reset()
	err = parquet.Write(b, []Element{
		{"Boron", "B", 5, 10.811},
		{"Carbon", "C", 6, 12.0107},
		{"Nitrogen", "N", 7, 14.0067},
		{"Oxygen", "O", 8, 15.9994},
	})
	require.NoError(t, err)

	require.NoError(t, iceberg.Upload(ctx, "db/table/ulid/data.parquet", b))

	iceberg.maxDataFileAge = time.Since(modtime)
	iceberg.orphanedFileAge = 1
	require.NoError(t, iceberg.Maintenance(ctx)) // This maintenance will delete the first file from the table; And then remove the file from the bucket

	require.NoError(t, bucket.Iter(ctx, "", func(name string) error {
		require.NotEqual(t, deleted, name)
		return nil
	}, objstore.WithRecursiveIter))
}
