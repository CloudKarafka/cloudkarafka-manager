package api

import (
	"fmt"
	"regexp"
)

var validConfigKeys = []string{
	"cleanup.policy",
	"compression.type",
	"delete.retention.ms",
	"file.delete.delay.ms",
	"flush.messages",
	"flush.ms",
	"follower.replication.throttled.replicas",
	"index.interval.bytes",
	"leader.replication.throttled.replicas",
	"max.message.bytes",
	"message.format.version",
	"message.timestamp.difference.max.ms",
	"message.timestamp.type",
	"min.cleanable.dirty.ratio",
	"min.compaction.lag.ms",
	"min.insync.replicas",
	"preallocate",
	"retention.bytes",
	"retention.ms",
	"segment.bytes",
	"segment.index.bytes",
	"segment.jitter.ms",
	"segment.ms",
	"unclean.leader.election.enable",
	"message.downconversion.enable",
}

type TopicModel struct {
	Name              string                 `json:"name"`
	PartitionCount    int                    `json:"partition_count"`
	ReplicationFactor int                    `json:"replication_factor"`
	Config            map[string]interface{} `json:"config"`
}

func (topic TopicModel) Validate() []string {
	res := make([]string, 0)
	if len(topic.Name) <= 0 || len(topic.Name) > 255 {
		res = append(res, "Topic name must be between 0 and 255 characters")
	}
	if match, _ := regexp.MatchString("\\A[A-Za-z0-9\\.\\-_]+\\z", topic.Name); !match {
		res = append(res, "Topic name can only contain letters, numbers and dot, underline and strike (., _, -).")
	}
	if topic.ReplicationFactor <= 0 {
		res = append(res, "Replication factor must be bigger than zero")
	}
	if topic.PartitionCount <= 0 {
		res = append(res, "Parition count must be bigger than zero")
	}
	return append(res, topic.validateConfig()...)
}

func (topic TopicModel) isValidConfigKey(key string) bool {
	for _, vck := range validConfigKeys {
		if key == vck {
			return true
		}
	}
	return false
}

func (topic TopicModel) validateConfig() []string {
	var errs []string
	for k, v := range topic.Config {
		if !topic.isValidConfigKey(k) {
			errs = append(errs, fmt.Sprintf("%s=%s", k, v))
		}
	}
	return errs

}
