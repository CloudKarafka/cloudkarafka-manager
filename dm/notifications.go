package dm

import (
	"cloudkarafka-mgmt/store"

	"fmt"
)

type notification struct {
	Type    string `json:"type"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

type notificationFn func() ([]notification, bool)

func Notifications() []notification {
	var notifications []notification
	fns := []notificationFn{
		checkUnevenPartitions,
		checkUnevenLeaders,
	}
	for _, fn := range fns {
		if n, any := fn(); any {
			notifications = append(notifications, n...)
		}
	}
	return notifications
}

func checkUneven(index, msg string) ([]notification, bool) {
	var (
		n   []notification
		any = false
	)
	grouped := store.
		SelectWithIndex(index).
		GroupBy(func(d store.Data) string { return d.Tags["broker"] })
	sum := 0
	for _, values := range grouped {
		values.Sort()
		value := values.Last()
		sum += value.Value
	}
	for id, values := range grouped {
		if sum == 0 {
			break
		}
		values.Sort()
		value := values.Last()
		q := float64(len(grouped)*value.Value) / float64(sum)
		var (
			level string
		)
		if q < 0.2 {
			level = "danger"
		} else if q < 0.5 {
			level = "warning"
		} else if q < 0.8 {
			level = "info"
		}
		if level != "" {
			n = append(n, notification{
				Type:    "Uneven spread",
				Level:   level,
				Message: fmt.Sprintf(msg, id, int(q*100), sum),
			})
			any = true
		}
	}
	return n, any
}

func checkUnevenPartitions() ([]notification, bool) {
	msg := "Your cluster has a uneven partition spread. Broker %s has %v%% of the total number (%v) of partitions. This will have negative impact on the throughput."
	return checkUneven("PartitionCount", msg)
}

func checkUnevenLeaders() ([]notification, bool) {
	msg := "Your cluster has a uneven leader spread. Broker %s is leader of %v%% of the total number (%v) of partitions. This will have negative impact on the throughput."
	return checkUneven("LeaderCount", msg)
}

func checkFailedLogins() ([]notification, bool) {
	any := store.SelectWithIndex("socket-server").Any(func(d store.Data) bool {
		return d.Tags["attr"] == "failed-authentication-total" && d.Value > 0
	})
	var n []notification
	if any {
		n = append(n, notification{
			Type:    "Failed authentication attempts",
			Message: "Failed authentication attempts, check the client logs.",
			Level:   "info",
		})
	}
	return n, any
}
