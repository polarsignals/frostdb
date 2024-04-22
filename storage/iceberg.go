package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/iceberg-go"
	"github.com/polarsignals/iceberg-go/catalog"
	"github.com/polarsignals/iceberg-go/table"
	"github.com/thanos-io/objstore"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/expr"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

/*
	Iceberg is an Apache Iceberg backed DataSink/DataSource.

	The Iceberg layout is as follows:
	<warehouse>/<database>/<table>/<table_version_number>.metadata.json   // Metadata file
	<warehouse>/<database>/<table>/data/<ulid>.parquet			          // data files
	<warehouse>/<database>/<table>/metadata/snap.<ulid>.avro	          // Manifest list file (snapshot)
	<warehouse>/<database>/<table>/metadata/<ulid>.avro		              // Manifest file
	<warehouse>/<database>/<table>/metadata/version-hint.text		      // Version hint file

	On Upload a new snapshot is created and the data file is added to a manifest (or a new manifest is created depending on settings).
	This manifest is then added to the existing manifest list, and a new version of the metadata file is created.

	Once the new metadata file is written the version hint file is updated with the latest version number of the table.
	This version-hint file is used to determine the latest version of the table.

	On Scan the latest snapshot is loaded and the manifest list is read.
	If the manifests are partitioned; the manifests are filtered out based on the given filter against the partition data.
	The remaining manifests are then read, and the data files are filtered out based on the given filter and the min/max columns of the data file.

	Remaining data files are then read and the filter is applied to each row group in the data file.

*/

// Iceberg is an Apache Iceberg backed DataSink/DataSource.
type Iceberg struct {
	catalog   catalog.Catalog
	bucketURI string // bucketURI is the URI of the bucket i.e gs://<bucket-name>, s3://<bucket-name> etc.
	bucket    objstore.Bucket
}

// NewIceberg creates a new Iceberg DataSink/DataSource.
// You must provide the URI of the warehouse and the objstore.Bucket that points to that warehouse.
func NewIceberg(uri string, ctlg catalog.Catalog, bucket objstore.Bucket) (*Iceberg, error) {
	return &Iceberg{
		catalog:   ctlg,
		bucketURI: uri,
		bucket:    catalog.NewIcebucket(uri, bucket),
	}, nil
}

func (i *Iceberg) String() string {
	return "Iceberg"
}

// Scan will load the latest Iceberg table. It will filter out any manifests that do not contain useful data.
// Then it will read the manifests that may contain useful data. It will then filter out the data file that dot not contain useful data.
// Finally it has a set of data files that may contain useful data. It will then read the data files and apply the filter to each row group in the data file.
func (i *Iceberg) Scan(ctx context.Context, prefix string, _ *dynparquet.Schema, filter logicalplan.Expr, _ uint64, callback func(context.Context, any) error) error {
	t, err := i.catalog.LoadTable(ctx, []string{i.bucketURI, prefix}, iceberg.Properties{})
	if err != nil {
		if errors.Is(catalog.ErrorTableNotFound, err) {
			return nil
		}
		return err
	}

	// Get the latest snapshot
	snapshot := t.CurrentSnapshot()
	list, err := snapshot.Manifests(i.bucket)
	if err != nil {
		return fmt.Errorf("error reading manifest list: %w", err)
	}

	fltr, err := expr.BooleanExpr(filter)
	if err != nil {
		return err
	}

	for _, manifest := range list {
		// TODO(thor): manifests can be filtered by partition data
		entries, err := manifest.FetchEntries(i.bucket, false)
		if err != nil {
			return err
		}

		for _, e := range entries {
			ok, err := manifestEntryMayContainUsefulData(icebergSchemaToParquetSchema(t.Schema()), e, fltr)
			if err != nil {
				return fmt.Errorf("failed to filter entry: %w", err)
			}
			if !ok {
				continue
			}

			// TODO(thor): data files can be processed in parallel
			bkt := NewBucketReaderAt(i.bucket)
			r, err := bkt.GetReaderAt(ctx, e.DataFile().FilePath())
			if err != nil {
				return err
			}

			file, err := parquet.OpenFile(
				r,
				e.DataFile().FileSizeBytes(),
				parquet.FileReadMode(parquet.ReadModeAsync),
			)
			if err != nil {
				return err
			}

			// Get a reader from the file bytes
			buf, err := dynparquet.NewSerializedBuffer(file)
			if err != nil {
				return err
			}

			for i := 0; i < buf.NumRowGroups(); i++ {
				rg := buf.DynamicRowGroup(i)
				mayContainUsefulData, err := fltr.Eval(rg)
				if err != nil {
					return err
				}
				if mayContainUsefulData {
					if err := callback(ctx, rg); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// Prefixes lists all the tables found in the warehouse for the given database(prefix).
func (i *Iceberg) Prefixes(ctx context.Context, prefix string) ([]string, error) {
	tables, err := i.catalog.ListTables(ctx, []string{filepath.Join(i.bucketURI, prefix)})
	if err != nil {
		return nil, err
	}

	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, filepath.Join(t...))
	}
	return tableNames, nil
}

// Upload a parquet file into the Iceberg table.
func (i *Iceberg) Upload(ctx context.Context, name string, r io.Reader) error {
	tablePath := filepath.Join(i.bucketURI, filepath.Dir(filepath.Dir(name)))
	t, err := i.catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
	if err != nil {
		if !errors.Is(catalog.ErrorTableNotFound, err) {
			return err
		}

		// Table doesn't exist, create it
		t, err = i.catalog.CreateTable(ctx, tablePath, iceberg.NewSchema(0), iceberg.Properties{})
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	w, err := t.SnapshotWriter(
		table.WithMergeSchema(),
		table.WithManifestSizeBytes(8*1024*1024), // 8MiB manifest size
	)
	if err != nil {
		return err
	}

	if err := w.Append(ctx, r); err != nil {
		return err
	}

	return w.Close(ctx)
}

func (i *Iceberg) Delete(_ context.Context, _ string) error {
	// Noop
	// NOTE: Deletes are used in DataSinks when an upload fails for any reason. Because an Iceberg table is not updated
	// until a full upload is successfull there is no risk of partial data being left in the table, or a corrupted file being read.
	return nil
}

func icebergSchemaToParquetSchema(schema *iceberg.Schema) *parquet.Schema {
	g := parquet.Group{}
	for _, f := range schema.Fields() {
		switch f.Type.Type() {
		case "long":
			g[f.Name] = parquet.Int(64)
		case "binary":
			g[f.Name] = parquet.String()
		}
	}
	return parquet.NewSchema("iceberg", g)
}

func manifestEntryMayContainUsefulData(schema *parquet.Schema, entry iceberg.ManifestEntry, filter expr.TrueNegativeFilter) (bool, error) {
	return filter.Eval(dataFileToParticulate(schema, entry.DataFile()))
}

func dataFileToParticulate(schema *parquet.Schema, d iceberg.DataFile) expr.Particulate {
	return &dataFileParticulate{
		schema: schema,
		data:   d,
	}
}

type dataFileParticulate struct {
	schema *parquet.Schema
	data   iceberg.DataFile
}

func (d *dataFileParticulate) Schema() *parquet.Schema {
	return d.schema
}

func (d *dataFileParticulate) ColumnChunks() []parquet.ColumnChunk {
	virtualColumnChunks := make([]parquet.ColumnChunk, 0, len(d.schema.Fields()))
	for i := range d.schema.Fields() {
		virtualColumnChunks = append(virtualColumnChunks, &virtualColumnChunk{
			pType:       d.schema.Fields()[i].Type(),
			nulls:       d.data.NullValueCounts()[i],
			column:      i,
			lowerBounds: d.data.LowerBoundValues()[i],
			upperBounds: d.data.UpperBoundValues()[i],
			numValues:   d.data.Count(),
		})
	}
	return virtualColumnChunks
}

type virtualColumnChunk struct {
	pType       parquet.Type
	column      int
	numValues   int64
	nulls       int64
	lowerBounds []byte
	upperBounds []byte
}

func (v *virtualColumnChunk) Type() parquet.Type   { return nil }
func (v *virtualColumnChunk) Column() int          { return v.column }
func (v *virtualColumnChunk) Pages() parquet.Pages { return nil }
func (v *virtualColumnChunk) ColumnIndex() (parquet.ColumnIndex, error) {
	return &virtualColumnIndex{
		pType:       v.pType,
		nulls:       v.nulls,
		lowerBounds: v.lowerBounds,
		upperBounds: v.upperBounds,
	}, nil
}
func (v *virtualColumnChunk) OffsetIndex() (parquet.OffsetIndex, error) { return nil, nil }
func (v *virtualColumnChunk) BloomFilter() parquet.BloomFilter          { return nil }
func (v *virtualColumnChunk) NumValues() int64                          { return v.numValues }

type virtualColumnIndex struct {
	lowerBounds []byte
	upperBounds []byte
	nulls       int64
	pType       parquet.Type
}

func (v *virtualColumnIndex) NumPages() int       { return 1 }
func (v *virtualColumnIndex) NullCount(int) int64 { return v.nulls }
func (v *virtualColumnIndex) NullPage(int) bool   { return false }
func (v *virtualColumnIndex) MinValue(int) parquet.Value {
	switch v.pType.Kind() {
	case parquet.Int64:
		i := binary.LittleEndian.Uint64(v.lowerBounds)
		return parquet.Int64Value(int64(i))
	case parquet.ByteArray:
		return parquet.ByteArrayValue(v.lowerBounds)
	default:
		return parquet.ByteArrayValue(v.lowerBounds)
	}
}

func (v *virtualColumnIndex) MaxValue(int) parquet.Value {
	switch v.pType.Kind() {
	case parquet.Int64:
		i := binary.LittleEndian.Uint64(v.upperBounds)
		return parquet.Int64Value(int64(i))
	case parquet.ByteArray:
		return parquet.ByteArrayValue(v.upperBounds)
	default:
		return parquet.ByteArrayValue(v.upperBounds)
	}
}

func (v *virtualColumnIndex) IsAscending() bool  { return true }
func (v *virtualColumnIndex) IsDescending() bool { return false }
