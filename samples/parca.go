package samples

type Parca struct {
	Duration       int64             `frostdb:"duration,rle_dict"`
	Labels         map[string]string `frostdb:"labels,rle_dict,asc(3),null_first"`
	Name           string            `frostdb:"name,rle_dict,asc(0)"`
	Period         int64             `frostdb:"period,rle_dict"`
	PeriodType     string            `frostdb:"period_type,rle_dict"`
	PeriodUnit     string            `frostdb:"period_unit,rle_dict"`
	PprofLabels    map[string]string `frostdb:"pprof_labels,rle_dict,asc(6),null_first"`
	PprofNumLabels int64             `frostdb:"pprof_num_labels,rle_dict,asc(7),null_first"`
	SampleType     string            `frostdb:"sample_type,rle_dict,asc(1)"`
	SampleUnit     string            `frostdb:"sample_unit,rle_dict,asc(2)"`
	StackTrace     string            `frostdb:"stacktrace,lz4_raw,asc(5)"`
	Timestamp      int64             `frostdb:"timestamp,lz4_raw,delta_binary_packed,asc(4)"`
	Value          int64             `frostdb:"value,lz4_raw,delta_binary_packed"`
}
