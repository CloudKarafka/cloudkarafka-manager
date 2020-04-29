package notifications

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
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
		conn, err := store.DialJMXServer()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] CheckURP: %s\n", err)
			return
		}
		r, err := conn.GetMetrics(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] CheckURP: %s\n", err)
			return
		}
		if r[0].Value > 0 {
			res = append(res, buildURPNotification(r[0]))
		}
	}
	out <- res
}

func CheckBalancedLeaders(out chan []Notification) {
	var (
		res       = make([]Notification, 0)
		stat      = make(map[int]int)
		total     = 0
		_, cancel = context.WithCancel(context.Background())
	)
	defer cancel()
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
		conn, err := store.DialJMXServer()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] CheckURP: %s\n", err)
			return
		}
		r, err := conn.GetMetrics(req)
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
	brokers := store.Brokers()
	stat := make([]IsrStat, len(brokers))
	for _, b := range brokers {
		stat[b.Id] = IsrStat{
			Expand: b.ISRExpand.Last(),
			Shrink: b.ISRShrink.Last(),
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
