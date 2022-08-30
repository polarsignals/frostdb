package main

import (
	"fmt"
	"log"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: parquet-tool <parquet-file> <new-schema> <output-file>")
		os.Exit(1)
	}

	parquetFile := os.Args[1]
	newSchemaFile := os.Args[2]
	outputFile := os.Args[3]

	newSchema, err := readSchema(newSchemaFile)
	if err != nil {
		log.Fatal(fmt.Errorf("read schema from file %q: %w", newSchemaFile, err))
	}

	pqf, err := os.Open(parquetFile)
	if err != nil {
		log.Fatal(fmt.Errorf("open file: %w", err))
	}

	fileInfo, err := pqf.Stat()
	if err != nil {
		log.Fatal(fmt.Errorf("stat parquet file: %w", err))
	}

	pqFile, err := parquet.OpenFile(pqf, fileInfo.Size())
	if err != nil {
		log.Fatal(fmt.Errorf("stat parquet file: %w", err))
	}

	var dyncols map[string][]string
	dynColString, exists := pqFile.Lookup(dynparquet.DynamicColumnsKey)
	if exists {
		var err error
		dyncols, err = dynparquet.DeserializeDynColumns(dynColString)
		if err != nil {
			log.Fatalf("failed to deserialize dynamic cols: %v", err)
		}
	}

	outf, err := os.Create(outputFile)
	if err != nil {
		log.Fatal(fmt.Errorf("create output file: %w", err))
	}

	w, err := newSchema.GetWriter(outf, dyncols)
	if err != nil {
		log.Fatal(fmt.Errorf("get writer: %w", err))
	}

	rowGroups := pqFile.RowGroups()
	for _, rg := range rowGroups {
		if _, err := parquet.CopyRows(w, rg.Rows()); err != nil {
			log.Fatal(fmt.Errorf("copy rows: %w", err))
		}
	}

	if err := w.Close(); err != nil {
		log.Fatal(fmt.Errorf("close parquet writer: %w", err))
	}

	if err := outf.Close(); err != nil {
		log.Fatal(fmt.Errorf("close output file: %w", err))
	}
}

func readSchema(file string) (*dynparquet.Schema, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	schema := &schemapb.Schema{}
	if err := (&jsonpb.Unmarshaler{}).Unmarshal(f, schema); err != nil {
		return nil, err
	}

	return dynparquet.SchemaFromDefinition(schema)
}
