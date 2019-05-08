package store

import (
	"bytes"
	"strings"
)

type JMXBean struct {
	Service string
	Params  map[string]string
}

func BeanFromString(str string) JMXBean {
	parts := strings.Split(str, ":")
	service := parts[0]
	params := parts[1]
	bean := JMXBean{
		Service: service,
		Params:  make(map[string]string),
	}
	for _, p := range strings.Split(params, ",") {
		pp := strings.Split(p, "=")
		bean.Params[pp[0]] = pp[1]
	}
	return bean
}

func (b JMXBean) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(b.Service + ":")
	for k, v := range b.Params {
		buffer.WriteString(k + "=" + v + ",")
	}
	s := buffer.String()
	return s[:len(s)-1]
}

var (
	BeanBrokerBytesInPerSec = JMXBean{"kafka.server", map[string]string{
		"type": "BrokerTopicMetrics",
		"name": "BytesInPerSec"}}
	BeanBrokerBytesOutPerSec = JMXBean{"kafka.server", map[string]string{
		"type": "BrokerTopicMetrics",
		"name": "BytesOutPerSec"}}
	BeanBrokerMessagesInPerSec = JMXBean{"kafka.server", map[string]string{
		"type": "BrokerTopicMetrics",
		"name": "MessagesInPerSec"}}
	BeanBrokerBytesRejectedPerSec = JMXBean{"kafka.server", map[string]string{
		"type": "BrokerTopicMetrics",
		"name": "BytesRejectedPerSec"}}

	BeanBrokerPartitionCount = JMXBean{"kafka.server", map[string]string{
		"type": "ReplicaManager",
		"name": "PartitionCount"}}
	BeanBrokerLeaderCount = JMXBean{"kafka.server", map[string]string{
		"type": "ReplicaManager",
		"name": "LeaderCount"}}

	BeanBrokerUnderReplicatedPartitions = JMXBean{"kafka.server", map[string]string{
		"type": "ReplicaManager",
		"name": "UnderReplicatedPartitions"}}
	BeanBrokerOfflinePartitionsCount = JMXBean{"kafka.controller", map[string]string{
		"type": "ReplicaManager",
		"name": "OfflinePartitionsCount",
	}}
	BeanBrokerConnections = JMXBean{"kafka.server", map[string]string{
		"type":             "socket-server-metrics",
		"listener":         "*",
		"networkProcessor": "*"}}
	// res, err := QueryBroker(brokerId, bean, "connection-count", "listener")

	BeanAllTopicsBytesInPerSec = JMXBean{"kafka.server", map[string]string{
		"type":  "BrokerTopicMetrics",
		"name":  "BytesInPerSec",
		"topic": "*"}}
	BeanAllTopicsBytesOutPerSec = JMXBean{"kafka.server", map[string]string{
		"type":  "BrokerTopicMetrics",
		"name":  "BytesOutPerSec",
		"topic": "*"}}

	BeanTopicBytesInPerSec = func(topic string) JMXBean {
		return JMXBean{"kafka.server", map[string]string{
			"type":  "BrokerTopicMetrics",
			"name":  "BytesInPerSec",
			"topic": topic}}
	}
	BeanTopicBytesOutPerSec = func(topic string) JMXBean {
		return JMXBean{"kafka.server", map[string]string{
			"type":  "BrokerTopicMetrics",
			"name":  "BytesOutPerSec",
			"topic": topic}}
	}

	BeanAllTopicsLogStart = JMXBean{"kafka.log", map[string]string{
		"type":      "Log",
		"name":      "LogStartOffset",
		"topic":     "*",
		"partition": "*"}}

	BeanAllTopicsLogEnd = JMXBean{"kafka.log", map[string]string{
		"type":      "Log",
		"name":      "LogEndOffset",
		"topic":     "*",
		"partition": "*"}}

	BeanAllTopicsLogSize = JMXBean{"kafka.log", map[string]string{
		"type":      "Log",
		"name":      "Size",
		"topic":     "*",
		"partition": "*"}}

	BeanTopicLogStart = func(topic string) JMXBean {
		return JMXBean{"kafka.log", map[string]string{
			"type":      "Log",
			"name":      "LogStartOffset",
			"topic":     topic,
			"partition": "*"}}

	}
	BeanTopicLogEnd = func(topic string) JMXBean {
		return JMXBean{"kafka.log", map[string]string{
			"type":      "Log",
			"name":      "LogEndOffset",
			"topic":     topic,
			"partition": "*"}}

	}
	BeanTopicLogSize = func(topic string) JMXBean {
		return JMXBean{"kafka.log", map[string]string{
			"type":      "Log",
			"name":      "Size",
			"topic":     topic,
			"partition": "*"}}

	}
)
