package frostdb

import "github.com/prometheus/client_golang/prometheus"

var (
	descTxHighWatermark = prometheus.NewDesc(
		"frostdb_tx_high_watermark",
		"The highest transaction number that has been released to be read",
		[]string{"db"}, nil,
	)
	descActiveBlockSize = prometheus.NewDesc(
		"frostdb_table_active_block_size",
		"Size of the active table block in bytes.",
		[]string{"db", "table"}, nil,
	)
)

// collector is a custom prometheus collector that exports metrics from live
// databases and tables.
type collector struct {
	s *ColumnStore
}

var _ prometheus.Collector = (*collector)(nil)

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descTxHighWatermark
	ch <- descActiveBlockSize
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	for _, dbName := range c.s.DBs() {
		db, err := c.s.GetDB(dbName)
		if err != nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(descTxHighWatermark, prometheus.GaugeValue, float64(db.HighWatermark()), dbName)
		for _, tableName := range db.TableNames() {
			table, err := db.GetTable(tableName)
			if err != nil {
				continue
			}
			activeBlock := table.ActiveBlock()
			if activeBlock == nil {
				continue
			}
			ch <- prometheus.MustNewConstMetric(descActiveBlockSize, prometheus.GaugeValue, float64(activeBlock.Size()), dbName, tableName)
		}
	}
}
