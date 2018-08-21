package dm

import (
	"cloudkarafka-mgmt/store"

	"sort"
)

func BrokerBytesIn(id string) Series {
	b := store.Broker(id)
	return throughputTimeseries(b.BytesInPerSec)
}

func BrokerBytesOut(id string) Series {
	b := store.Broker(id)
	return throughputTimeseries(b.BytesOutPerSec)
}

func BrokerMessagesIn(id string) Series {
	b := store.Broker(id)
	return throughputTimeseries(b.MessagesInPerSec)
}

func TopicBytesIn(name string) Series {
	t := store.Topic(name)
	return throughputTimeseries(t.BytesInPerSec)
}

func TopicBytesOut(name string) Series {
	t := store.Topic(name)
	return throughputTimeseries(t.BytesOutPerSec)
}

func TopicMessagesIn(name string) Series {
	t := store.Topic(name)
	return throughputTimeseries(t.MessagesInPerSec)
}

func throughputTimeseries(timeseries store.Timeseries) Series {
	var (
		series = make(Series, len(timeseries))
		i      = 0
	)
	for ts, val := range timeseries {
		series[i] = DataPoint{Y: val, X: ts}
		i++
	}
	sort.Sort(series)
	return series
}
