package store

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	humanize "github.com/dustin/go-humanize"
)

type storage struct {
	sync.RWMutex
	brokers   brokers
	topics    topics
	consumers ConsumerGroups
}

var DB = &storage{
	brokers: make(brokers),
	topics:  make(topics),
}

func (me *storage) DeleteTopic(name string) {
	delete(me.topics, name)
}

func (me *storage) UpdateBroker(b broker) {
	me.Lock()
	defer me.Unlock()
	me.brokers[b.Id] = b
}

func (me *storage) Brokers() brokers {
	me.RLock()
	defer me.RUnlock()
	return me.brokers
}

func (me *storage) Broker(id int) (broker, bool) {
	me.RLock()
	defer me.RUnlock()
	b, ok := me.brokers[id]
	return b, ok
}

func (me *storage) Topics() TopicSlice {
	me.RLock()
	defer me.RUnlock()
	var (
		topics = make(TopicSlice, len(me.topics))
		i      = 0
	)
	for _, t := range me.topics {
		topics[i] = t
		i += 1
	}
	return topics
}
func (me *storage) Topic(name string) (topic, bool) {
	me.RLock()
	defer me.RUnlock()
	t, ok := me.topics[name]
	return t, ok
}
func (me *storage) UpdateTopic(t topic) {
	me.Lock()
	defer me.Unlock()
	me.topics[t.Name] = t
}
func (me *storage) UpdateTopicMetric(m Metric) {
	me.Lock()
	defer me.Unlock()
	t, ok := me.topics[m.Topic]
	if !ok {
		return
	}
	if m.Partition != "" {
		number, err := strconv.Atoi(m.Partition)
		if err != nil {
			return
		}
		if len(t.Partitions) <= number {
			fmt.Printf("[WARN] store.UpdateTopicMetric missing-partition topic=%s partition=%v\n", t.Name, number)
			return
		}
		p := t.Partitions[number]
		if p.Metrics == nil {
			p.Metrics = make(map[string]int)
		}
		p.Metrics[m.Name] = int(m.Value)
		for _, cg := range me.consumers {
			cg.UpdateTopic(m.Topic, number, m.Name, int(m.Value))
		}
	} else {
		fmt.Println(t.Name, m.Name, m.Value, m)
		switch m.Name {
		case "BytesInPerSec":
			t.BytesIn.Add(int(m.Value))
		case "BytesOutPerSec":
			t.BytesOut.Add(int(m.Value))
		}
	}
	//me.topics[m.Topic] = t
}

func (me *storage) BrokerTopicStats(brokerId int) (int, int, string) {
	me.RLock()
	defer me.RUnlock()
	var (
		partitionCount int
		leaderCount    int
		size           int
	)
	for _, t := range me.topics {
		for _, p := range t.Partitions {
			if p.Leader == brokerId {
				leaderCount += 1
			}
			sort.Ints(p.Replicas)
			if sort.SearchInts(p.Replicas, brokerId) != len(p.Replicas) {
				partitionCount += 1
				size += p.Metrics["Size"]
			}
		}
	}
	return partitionCount, leaderCount, humanize.Bytes(uint64(size))
}

func (me *storage) UpdateBrokerVersion(v Version) {
	me.Lock()
	defer me.Unlock()
	b, ok := me.brokers[v.Broker]
	if !ok {
		return
	}
	b.KafkaVersion = v.Version
}

func (me *storage) UpdateBrokerMetrics(m Metric) {
	me.Lock()
	defer me.Unlock()
	b, ok := me.brokers[m.Broker]
	if !ok {
		return
	}
	switch m.Name {
	case "BytesInPerSec":
		b.BytesIn.Add(int(m.Value))
	case "BytesOutPerSec":
		b.BytesOut.Add(int(m.Value))
	case "IsrExpandsPerSec":
		b.ISRShrink.Add(int(m.Value))
	case "IsrShrinksPerSec":
		b.ISRExpand.Add(int(m.Value))
	}
}
func (me *storage) SumBrokerSeries(metric string) TimeSerie {
	me.RLock()
	defer me.RUnlock()
	var (
		series = make([]TimeSerie, len(me.brokers))
		i      = 0
	)
	for _, b := range me.brokers {
		var s TimeSerie
		switch metric {
		case "bytes_in":
			s = b.BytesIn
		case "bytes_out":
			s = b.BytesOut
		case "isr_expand":
			s = b.ISRExpand
		case "isr_shrink":
			s = b.ISRShrink
		}
		series[i] = s
		i += 1
	}
	if len(series) == 0 {
		return &SumTimeSerie{}
	}
	return NewSumTimeSerie(series)
}

func (me *storage) Consumers() ConsumerGroups {
	me.RLock()
	defer me.RUnlock()
	return me.consumers
}

func (me *storage) Consumer(name string) (ConsumerGroup, bool) {
	me.RLock()
	defer me.RUnlock()
	for _, cg := range me.consumers {
		if cg.Name == name {
			return cg, true
		}
	}
	return ConsumerGroup{}, false
}

func (me *storage) UpdateConsumer(cg ConsumerGroup) {
	me.Lock()
	defer me.Unlock()
	cg.LastSeen = time.Now().Unix()
	for i, scg := range me.consumers {
		if scg.Name == cg.Name {
			for _, c := range scg.Clients {
				cg.UpdateTopic(c.Topic, c.Partition, "LogEndOffset", c.LogEnd)
				cg.UpdateTopic(c.Topic, c.Partition, "LogStartOffset", c.LogStart)
			}
			me.consumers[i] = cg
			return
		}
	}
	me.consumers = append(me.consumers, cg)
}

func Uptime() string {
	var ts int64
	for _, b := range DB.Brokers() {
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

func Partitions() int {
	count := 0
	for _, t := range DB.Topics() {
		count += len(t.Partitions)
	}
	return count
}
func TotalTopicSize() string {
	size := 0
	for _, t := range DB.Topics() {
		size += t.Size()
	}
	return humanize.Bytes(uint64(size))
}
func TotalMessageCount() int {
	msgs := 0
	for _, t := range DB.Topics() {
		msgs += t.Messages()
	}
	return msgs
}

func UpdateTopic(name string) bool {
	t, err := FetchTopic(name)
	if err != nil {
		return false
	}
	DB.UpdateTopic(t)
	return true
}
