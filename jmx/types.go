package jmx

type TransferMetric struct {
	BytesInPerSec    float64 `json:"bytes_in_per_sec"`
	BytesOutPerSec   float64 `json:"bytes_out_per_sec"`
	MessagesInPerSec float64 `json:"messages_in_per_sec"`
}

type TopicMetric struct {
	TransferMetric
	MessageCount int `json:"message_count"`
}

type BrokerMetric struct {
	TransferMetric
	KafkaVersion string `json:"kafka_version"`
	BrokerId     string `json:"broker_id"`
}

type OffsetMetric struct {
	LogEndOffset   int `json:"log_end_offset"`
	LogStartOffset int `json:"log_start_offset"`
}
