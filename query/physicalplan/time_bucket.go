package physicalplan

import (
	"fmt"
	"math"
	"time"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

var (
	defaultOrigin, _ = time.Parse(time.DateOnly, "2000-01-03")
	defaultOriginMs  = defaultOrigin.UnixMilli()
)

const (
	minTs = math.MinInt64
	maxTs = math.MinInt64
)

func newTimeBucketHash(period logicalplan.Period) *bucketHashCombine {
	switch e := period.(type) {
	case logicalplan.Day:
		return &bucketHashCombine{
			period: (time.Hour * 24 * time.Duration(e)).Milliseconds(),
			hash:   bucketDay,
		}
	case logicalplan.Week:
		return &bucketHashCombine{
			period: (time.Hour * 24 * 7 * time.Duration(e)).Milliseconds(),
			hash:   bucketWeek,
		}
	case logicalplan.Month:
		return &bucketHashCombine{
			period: (time.Hour * 24 * 30 * time.Duration(e)).Milliseconds(),
			hash:   bucketMonth,
		}
	case logicalplan.Year:
		return &bucketHashCombine{
			period: (time.Hour * 24 * 365 * time.Duration(e)).Milliseconds(),
			hash:   bucketYear,
		}
	default:
		panic(fmt.Sprintf("time_bucket: unexpected period type %T", period))
	}
}

type bucketHashCombine struct {
	period int64
	hash   func(int64, time.Time) time.Time
}

func (b *bucketHashCombine) hashCombine(v uint64) uint64 {
	ts := time.UnixMilli(int64(v))
	yy, mm, dd := ts.Date()
	date := time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC)
	return uint64(b.hash(b.period, date).UnixMilli())
}

func bucketWeek(period int64, date time.Time) time.Time {
	r := timeBucket(
		period, date.UnixMilli(), defaultOriginMs,
	)
	yy, mm, dd := time.UnixMilli(r).UTC().Date()
	day := time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC)
	return day.AddDate(0, 0, -int(day.Weekday()))
}

func bucketDay(period int64, date time.Time) time.Time {
	r := timeBucket(
		period, date.UnixMilli(), defaultOriginMs,
	)
	yy, mm, dd := time.UnixMilli(r).UTC().Date()
	return time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC)
}

func bucketMonth(period int64, date time.Time) time.Time {
	r := timeBucket(
		period, date.UnixMilli(), defaultOriginMs,
	)
	yy, mm, _ := time.UnixMilli(r).UTC().Date()
	return time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC)
}

func bucketYear(period int64, date time.Time) time.Time {
	r := timeBucket(
		period, date.UnixMilli(), defaultOriginMs,
	)
	yy, _, _ := time.UnixMilli(r).UTC().Date()
	return time.Date(yy, time.January, 1, 0, 0, 0, 0, time.UTC)
}

func timeBucket(period, timestamp, offset int64) (result int64) {
	if period <= 0 {
		panic("time_bucket: period must be greater than 0")
	}
	if offset != 0 {
		offset = offset % period
		if (offset > 0 && timestamp < minTs+offset) ||
			(offset < 0 && timestamp > maxTs+offset) {
			panic("time_bucket: timestamp out of range")
		}
		timestamp -= offset
	}
	result = (timestamp / period) * period
	if timestamp < 0 && (timestamp%period) != 0 {
		if result < minTs+period {
			panic("time_bucket: timestamp out of range")
		}
		result -= period
	}
	result += offset
	return
}
