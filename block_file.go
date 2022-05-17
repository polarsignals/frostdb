package arcticdb

import (
	"errors"
	"os"
)

type BlockFile struct {
	*os.File
	size uint64
}

func OpenBlockFile(name string) (*BlockFile, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	size := uint64(0)
	fstat, err := os.Stat(name)
	if err == nil {
		size = uint64(fstat.Size())
	}

	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}

	return &BlockFile{
		File: file,
		size: size,
	}, err
}
