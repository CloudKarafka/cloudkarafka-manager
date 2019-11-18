package notifications

import (
	"fmt"
	"os"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
)

func buildNotification(brokerId int, level Level, key, title, message string) Notification {
	return Notification{
		Key:       fmt.Sprintf("%s-%d", key, brokerId),
		Title:     title,
		Level:     level,
		Message:   message,
		Timestamp: time.Now(),
		Link: NotificationLink{
			fmt.Sprintf("broker/%d", brokerId),
			"Go to broker"},
	}
}
func buildURPNotification(m store.Metric) Notification {
	return buildNotification(
		m.Broker,
		WARNING,
		"URP",
		"Under replicated partitions",
		fmt.Sprintf("Broker %d has %d under replicated partitions", m.Broker, int(m.Value)))
}

func CheckURP(out chan []Notification) {
	res := make([]Notification, 0)
	for brokerId, _ := range config.BrokerUrls {
		req := store.MetricRequest{
			brokerId,
			store.BeanBrokerUnderReplicatedPartitions,
			"Value",
		}
		r, err := store.GetMetrics(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] CheckURP: %s\n", err)
		} else {
			if r[0].Value > 0 {
				res = append(res, buildURPNotification(r[0]))
			}
		}
	}
	out <- res
}

func CheckBalancedLeaders(out chan []Notification) {
	res := make([]Notification, 0)
	stat := make(map[int]int)
	total := 0
	if len(config.BrokerUrls) == 1 {
		out <- res
		return
	}
	for brokerId, _ := range config.BrokerUrls {
		req := store.MetricRequest{
			brokerId,
			store.BeanBrokerLeaderCount,
			"Value",
		}
		r, err := store.GetMetrics(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] CheckBalancedLeader: %s\n", err)
		} else {
			stat[brokerId] = int(r[0].Value)
			total += int(r[0].Value)
		}
	}
	for b, s := range stat {
		percent := float64(s) / float64(total)
		if percent > float64(0.8) {
			res = append(res, buildNotification(
				b,
				WARNING,
				"LEAD_BALANCE",
				"Unbalanced leaders",
				fmt.Sprintf("Broker %d is leader for %.0f%% of partitions", b, 100*percent)))
		}
	}
	out <- res
}

type IsrStat struct {
	Shrink int
	Expand int
}

func (me IsrStat) Diff() int {
	return me.Expand - me.Shrink
}

func CheckISRDelta(out chan []Notification) {
	res := make([]Notification, 0)
	stat := make(map[int]IsrStat)
	metrics := make([]store.MetricRequest, 0)
	beans := []store.JMXBean{store.BeanBrokerIsrExpands, store.BeanBrokerIsrShrinks}
	l := 0
	for brokerId, _ := range config.BrokerUrls {
		for _, bean := range beans {
			metrics[l] = store.MetricRequest{brokerId, bean, "OneMinuteRate"}
			l += 1
		}
	}
	ch := store.GetMetricsAsync(metrics)
	for i := 0; i < l; i++ {
		select {
		case response := <-ch:
			if response.Error != nil {
				log.Error("CheckISRDelta", log.ErrorEntry{response.Error})
			} else {
				for _, metric := range response.Metrics {
					if _, ok := stat[metric.Broker]; !ok {
						stat[metric.Broker] = IsrStat{}
					}
					a := stat[metric.Broker]
					if metric.Name == "IsrExpandsPerSec" {
						a.Expand = int(metric.Value)
					} else {
						a.Shrink = int(metric.Value)
					}
					stat[metric.Broker] = a
				}
			}
			//case <-ctx.Done():
			//return res, fmt.Errorf("Fetching broker metrics failed: %s", ctx.Err())
		}

	}
	for b, s := range stat {
		if s.Diff() > 0 {
			res = append(res, buildNotification(
				b,
				WARNING,
				"ISR_DELTA",
				"ISR Delta",
				fmt.Sprintf("Broker %d is expanding and shrinking the ISR frequently", b)))
		}
	}
	out <- res
}
