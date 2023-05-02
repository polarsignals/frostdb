package frostdb

func WithTestingNoDiskSpaceReclaimOnSnapshot() Option {
	return func(s *ColumnStore) error {
		s.testingOptions.disableReclaimDiskSpaceOnSnapshot = true
		return nil
	}
}
