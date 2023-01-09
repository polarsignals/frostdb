package bufutils

import "sort"

// Dedupe deduplicates the slices of values for each key in the map.
func Dedupe(s map[string][]string) map[string][]string {
	final := map[string][]string{}
	set := map[string]map[string]struct{}{}
	for k, v := range s {
		if set[k] == nil {
			set[k] = map[string]struct{}{}
		}
		for _, i := range v {
			if _, ok := set[k][i]; !ok {
				set[k][i] = struct{}{}
				final[k] = append(final[k], i)
			}
		}
	}

	for _, s := range final {
		sort.Strings(s)
	}
	return final
}
