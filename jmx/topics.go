package jmx

import (
	"fmt"
)

func TopicMessageCount(topic string, partitions []string) int {
	msgs := 0
	for _, p := range partitions {
		lo, err := LogOffsetMetric(topic, p)
		if err != nil {
			fmt.Println(err)
		}
		msgs += lo.LogEndOffset - lo.LogStartOffset
	}
	return msgs
}

func LogOffsetMetric(t, p string) (OffsetMetric, error) {
	var om OffsetMetric
	so, err := LogOffset("LogStartOffset", t, p)
	if err != nil {
		return om, nil
	}
	eo, err := LogOffset("LogEndOffset", t, p)
	if err != nil {
		return om, nil
	}
	om = OffsetMetric{
		LogStartOffset: so,
		LogEndOffset:   eo,
	}
	return om, nil
}
