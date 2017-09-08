package jmx

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
