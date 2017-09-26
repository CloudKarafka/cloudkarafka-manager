package jmx

import (
	"fmt"
)

func TopicMetrics(t string) (TransferMetric, error) {
	var tm TransferMetric
	bi, err := BrokerTopicMetric("BytesInPerSec", t)
	if err != nil {
		return tm, err
	}
	bo, err := BrokerTopicMetric("BytesOutPerSec", t)
	if err != nil {
		return tm, err
	}
	mi, err := BrokerTopicMetric("MessagesInPerSec", t)
	if err != nil {
		return tm, err
	}
	tm = TransferMetric{
		BytesInPerSec:    bi,
		BytesOutPerSec:   bo,
		MessagesInPerSec: mi,
	}
	return tm, nil
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

func TopicMessageCount(topic string, partitions []string) int {
	msgs := 0
	for _, p := range partitions {
		s, err := LogOffset("LogStartOffset", topic, p)
		if err != nil {
			fmt.Println(err)
		}
		e, err := LogOffset("LogEndOffset", topic, p)
		if err != nil {
			fmt.Println(err)
		}
		msgs += e - s
	}
	return msgs
}