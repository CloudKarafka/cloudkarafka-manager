package dm

import (
	"cloudkarafka-mgmt/store"
)

func ThroughputTimeseries(metric, id string) Series {
	indexes := []string{id, metric}
	s := store.Intersection(indexes...)
	total := make(map[int64]int)
	s.Each(func(d store.Data) {
		total[d.Timestamp] += d.Value
	})
	series := make(Series, len(total))
	i := 0
	for x, y := range total {
		series[i] = DataPoint{X: x, Y: y}
		i += 1
	}
	series.Sort()
	return series
}
