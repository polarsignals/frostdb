// Copyright (c) The FrostDB Authors.
// Licensed under the Apache License 2.0.
// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storage

import (
	"context"
	"errors"
	"io"

	"github.com/thanos-io/objstore"
)

// Bucket is an objstore.Bucket that also supports reading files via a ReaderAt interface.
type Bucket interface {
	objstore.Bucket
	GetReaderAt(ctx context.Context, name string) (io.ReaderAt, error)
}

// FileReaderAt is a wrapper around a objstore.Bucket that implements the ReaderAt interface.
type FileReaderAt struct {
	objstore.Bucket
	name string
	ctx  context.Context
}

// BucketReaderAt implements the Bucket interface.
type BucketReaderAt struct {
	objstore.Bucket
}

// NewBucketReaderAt returns a new Bucket.
func NewBucketReaderAt(bucket objstore.Bucket) *BucketReaderAt {
	return &BucketReaderAt{Bucket: bucket}
}

// GetReaderAt returns a io.ReaderAt for the given filename.
func (b *BucketReaderAt) GetReaderAt(ctx context.Context, name string) (io.ReaderAt, error) {
	return &FileReaderAt{
		Bucket: b.Bucket,
		name:   name,
		ctx:    ctx,
	}, nil
}

// ReadAt implements the io.ReaderAt interface.
func (b *FileReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	rc, err := b.GetRange(b.ctx, b.name, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer func() {
		rc.Close()
	}()

	total := 0
	for total < len(p) { // Read does not guarantee the buffer will be full, but ReadAt does
		n, err = rc.Read(p[total:])
		total += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				// If io.EOF is returned it means we read the end of the file and simply return the total.
				break
			}
			return total, err
		}
	}

	return total, nil
}
