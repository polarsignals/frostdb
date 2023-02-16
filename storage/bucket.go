// Copyright (c) The FrostDB Authors.
// Licensed under the Apache License 2.0.
// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storage

import (
	"context"
	"io"
	"strings"

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
		// If io.EOF is returned it means we read the end of the file and simply return the total
		if err != nil && err != io.EOF {
			return total, err
		}
	}

	return total, nil
}

// PrefixedBucket is a Bucket object that has file names prefixed with a given path.
type PrefixedBucket struct {
	bkt    Bucket
	prefix string
}

// NewPrefixedBucket returns a new prefixed bucket.
func NewPrefixedBucket(bkt Bucket, prefix string) Bucket {
	if validPrefix(prefix) {
		return &PrefixedBucket{bkt: bkt, prefix: strings.Trim(prefix, objstore.DirDelim)}
	}

	return bkt
}

func validPrefix(prefix string) bool {
	prefix = strings.Replace(prefix, "/", "", -1)
	return len(prefix) > 0
}

func conditionalPrefix(prefix, name string) string {
	if len(name) > 0 {
		return withPrefix(prefix, name)
	}

	return name
}

func withPrefix(prefix, name string) string {
	return prefix + objstore.DirDelim + name
}

// GetReaderAt returns a io.ReaderAt object for the given file.
func (p *PrefixedBucket) GetReaderAt(ctx context.Context, name string) (io.ReaderAt, error) {
	return p.bkt.GetReaderAt(ctx, conditionalPrefix(p.prefix, name))
}

// Close implements the io.Closer interface.
func (p *PrefixedBucket) Close() error {
	return p.bkt.Close()
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
// Entries are passed to function in sorted order.
func (p *PrefixedBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	pdir := withPrefix(p.prefix, dir)

	return p.bkt.Iter(ctx, pdir, func(s string) error {
		return f(strings.TrimPrefix(s, p.prefix+objstore.DirDelim))
	}, options...)
}

// Get returns a reader for the given object name.
func (p *PrefixedBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return p.bkt.Get(ctx, conditionalPrefix(p.prefix, name))
}

// GetRange returns a new range reader for the given object name and range.
func (p *PrefixedBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return p.bkt.GetRange(ctx, conditionalPrefix(p.prefix, name), off, length)
}

// Exists checks if the given object exists in the bucket.
func (p *PrefixedBucket) Exists(ctx context.Context, name string) (bool, error) {
	return p.bkt.Exists(ctx, conditionalPrefix(p.prefix, name))
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (p *PrefixedBucket) IsObjNotFoundErr(err error) bool {
	return p.bkt.IsObjNotFoundErr(err)
}

// Attributes returns information about the specified object.
func (p PrefixedBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return p.bkt.Attributes(ctx, conditionalPrefix(p.prefix, name))
}

// Upload the contents of the reader as an object into the bucket.
// Upload should be idempotent.
func (p *PrefixedBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	return p.bkt.Upload(ctx, conditionalPrefix(p.prefix, name), r)
}

// Delete removes the object with the given name.
// If object does not exists in the moment of deletion, Delete should throw error.
func (p *PrefixedBucket) Delete(ctx context.Context, name string) error {
	return p.bkt.Delete(ctx, conditionalPrefix(p.prefix, name))
}

// Name returns the bucket name for the provider.
func (p *PrefixedBucket) Name() string {
	return p.bkt.Name()
}
