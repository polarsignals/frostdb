package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/spf13/cobra"

	"github.com/polarsignals/frostdb/dynparquet"
	snapshotpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/snapshot/v1alpha1"
)

var (
	snapshotMagic = "FDBS"

	snapshotVersion = 1
	minReadVersion  = snapshotVersion
)

var snapshotCmd = &cobra.Command{
	Use:     "snapshot",
	Example: "parquet-tool snapshot </path/to/snapshot/directory> [columns to dump]",
	Short:   "Interact with a snapshot directory",
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return inspectSnapshots(args[0], args[1:]...)
	},
}

func inspectSnapshots(dir string, columns ...string) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		return inspectSnapshot(path, info.Size(), columns)
	})
}

func inspectSnapshot(path string, size int64, columns []string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	footer, err := readFooter(f, size)
	if err != nil {
		return err
	}

	for _, tableMeta := range footer.TableMetadata {
		for _, granuleMeta := range tableMeta.GranuleMetadata {
			for _, partMeta := range granuleMeta.PartMetadata {
				startOffset := partMeta.StartOffset
				endOffset := partMeta.EndOffset
				partBytes := make([]byte, endOffset-startOffset)
				if _, err := f.ReadAt(partBytes, startOffset); err != nil {
					return err
				}
				switch partMeta.Encoding {
				case snapshotpb.Part_ENCODING_PARQUET:
					_, err := dynparquet.ReaderFromBytes(partBytes) // TODO: do something with the serialized buffer
					if err != nil {
						return err
					}
					fmt.Println("Parquet: ", partMeta.CompactionLevel)
				case snapshotpb.Part_ENCODING_ARROW:
					arrowReader, err := ipc.NewReader(bytes.NewReader(partBytes))
					if err != nil {
						return err
					}

					record, err := arrowReader.Read()
					if err != nil {
						return err
					}

					inspectRecord(record, columns)
				default:
					return fmt.Errorf("unknown part encoding: %s", partMeta.Encoding)
				}
			}
		}
	}

	return nil
}

// Copied from FrostDB directly
func readFooter(r io.ReaderAt, size int64) (*snapshotpb.FooterData, error) {
	buffer := make([]byte, 16)
	if _, err := r.ReadAt(buffer[:4], 0); err != nil {
		return nil, err
	}
	if string(buffer[:4]) != snapshotMagic {
		return nil, fmt.Errorf("invalid snapshot magic: %q", buffer[:4])
	}
	if _, err := r.ReadAt(buffer, size-int64(len(buffer))); err != nil {
		return nil, err
	}
	if string(buffer[12:]) != snapshotMagic {
		return nil, fmt.Errorf("invalid snapshot magic: %q", buffer[4:])
	}

	// The checksum does not include the last 8 bytes of the file, which is the
	// magic and the checksum. Create a section reader of all but the last 8
	// bytes to compute the checksum and validate it against the read checksum.
	checksum := binary.LittleEndian.Uint32(buffer[8:12])
	checksumWriter := newChecksumWriter()
	if _, err := io.Copy(checksumWriter, io.NewSectionReader(r, 0, size-8)); err != nil {
		return nil, fmt.Errorf("failed to compute checksum: %w", err)
	}
	if checksum != checksumWriter.Sum32() {
		return nil, fmt.Errorf(
			"snapshot file corrupt: invalid checksum: expected %x, got %x", checksum, checksumWriter.Sum32(),
		)
	}

	version := binary.LittleEndian.Uint32(buffer[4:8])
	if int(version) > snapshotVersion {
		return nil, fmt.Errorf(
			"cannot read snapshot with version %d: max version supported: %d", version, snapshotVersion,
		)
	}
	if int(version) < minReadVersion {
		return nil, fmt.Errorf(
			"cannot read snapshot with version %d: min version supported: %d", version, minReadVersion,
		)
	}

	footerSize := binary.LittleEndian.Uint32(buffer[:4])
	footerBytes := make([]byte, footerSize)
	if _, err := r.ReadAt(footerBytes, size-(int64(len(buffer))+int64(footerSize))); err != nil {
		return nil, err
	}
	footer := &snapshotpb.FooterData{}
	if err := footer.UnmarshalVT(footerBytes); err != nil {
		return nil, fmt.Errorf("could not unmarshal footer: %v", err)
	}
	return footer, nil
}

func newChecksumWriter() hash.Hash32 {
	return crc32.New(crc32.MakeTable(crc32.Castagnoli))
}
