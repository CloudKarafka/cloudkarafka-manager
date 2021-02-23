package api

import (
	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

func topics(fn zookeeper.PermissionFunc) store.TopicSlice {
	var (
		topics = store.Topics()
		i      = 0
	)
	for i < len(topics) {
		if !fn(topics[i].Name) {
			topics = append(topics[:i], topics[i+1:]...)
		} else {
			i += 1
		}
	}
	return topics
}

func consumers(fn zookeeper.PermissionFunc) store.ConsumerSlice {
	var (
		consumers   = store.Consumers()
		i           = 0
		beforeCount = len(consumers)
	)

	for i < len(consumers) {
		if !fn(consumers[i].Name) {
			consumers = append(consumers[:i], consumers[i+1:]...)
		} else {
			i += 1
		}
	}
	if config.VerboseLogging {
		log.Debug("store consumers", log.MapEntry{
			"before_filter": beforeCount,
			"after_filter":  len(consumers),
		})
	}
	return consumers
}
