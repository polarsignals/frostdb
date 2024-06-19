package frostdb

import "github.com/polarsignals/frostdb/wal"

type TestingOption Option

func WithTestingOptions(opts ...TestingOption) Option {
	return func(c *ColumnStore) error {
		for _, opt := range opts {
			if err := opt(c); err != nil {
				return err
			}
		}
		return nil
	}
}

func WithTestingNoDiskSpaceReclaimOnSnapshot() TestingOption {
	return func(c *ColumnStore) error {
		c.testingOptions.disableReclaimDiskSpaceOnSnapshot = true
		return nil
	}
}

func WithTestingWalOptions(opts ...wal.Option) TestingOption {
	return func(c *ColumnStore) error {
		c.testingOptions.walTestingOptions = opts
		return nil
	}
}
