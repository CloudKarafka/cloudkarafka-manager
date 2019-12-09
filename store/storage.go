package store

import (
	"strconv"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
)

type storage struct {
	Brokers   brokers
	Topics    topics
	Consumers ConsumerGroups
}

var store = storage{
	Brokers:   make(brokers),
	Topics:    make(topics),
	Consumers: make(ConsumerGroups),
}

func Uptime() string {
	var ts int64
	for _, b := range store.Brokers {
		tnew, err := strconv.ParseInt(b.Timestamp, 10, 64)
		if err != nil {
			continue
		}
		if ts == 0 || ts < tnew {
			ts = tnew
		}
	}
	if ts == 0 {
		return ""
	}
	return strings.TrimSpace(humanize.RelTime(time.Now(), time.Unix(ts/1000, 0), "", ""))
}

func Brokers() int {
	return len(store.Brokers)
}
func Topics() int {
	return len(store.Topics)
}
func Consumers() int {
	return len(store.Consumers)
}
func Partitions() int {
	count := 0
	for _, t := range store.Topics {
		count += len(t.Partitions)
	}
	return count
}
func TotalTopicSize() int {
	size := 0
	for _, t := range store.Topics {
		size += t.Size()
	}
	return size
}
func TotalMessageCount() int {
	msgs := 0
	for _, t := range store.Topics {
		msgs += t.Messages()
	}
	return msgs
}
