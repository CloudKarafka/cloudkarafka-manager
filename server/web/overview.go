package web

import (
	"context"
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	n "github.com/cloudkarafka/cloudkarafka-manager/notifications"
	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	humanize "github.com/dustin/go-humanize"
)

type OverviewItem struct {
	Title  string
	Value  int
	Icon   string
	format string
}

func (me OverviewItem) Format() string {
	switch me.format {
	case "size":
		return humanize.Bytes(uint64(me.Value))
	default:
		return humanize.Comma(int64(me.Value))
	}
}

func topicOverview(ctx context.Context, p zookeeper.Permissions) []OverviewItem {
	var (
		res            = []OverviewItem{}
		topicSize      = OverviewItem{"Total Topic Size", 0, "inbox", "size"}
		topicMsgCount  = OverviewItem{"Total Topic Message Count", 0, "mail-read", "count"}
		topicCount     = OverviewItem{"Topic Count", 0, "three-bars", "count"}
		partitionCount = OverviewItem{"Partition Count", 0, "three-bars", "count"}
	)
	topicNames, err := zookeeper.Topics(p)
	if err != nil {
		return nil
	}

	topics := make([]store.Topic, len(topicNames))
	for i, topicName := range topicNames {
		topics[i], err = store.FetchTopic(ctx, topicName, false, nil)
		if err != nil {
			log.Info("web.topicOverview", log.ErrorEntry{err})
			return nil
		}
	}
	for _, topic := range topics {
		topicSize.Value += topic.Size()
		topicMsgCount.Value += topic.Messages()
		partitionCount.Value += len(topic.Partitions)
		topicCount.Value += 1
	}
	return append(res, topicCount, partitionCount, topicSize, topicMsgCount)
}

func brokerOverview() []OverviewItem {
	return []OverviewItem{
		OverviewItem{"Broker Count", len(config.BrokerUrls), "pulse", "count"},
	}
}

/*
// TODO Show only consumer that consumes from topics that user has permissions for?
func consumerOverview(ctx context.Context, res map[string]int) map[string]int {
    res["consumer_count"] = 0
    if v, err := m.FetchConsumerGroups(ctx); err == nil {
        res["consumer_count"] = len(v)
    }
    return res
}

func userOverview(p zookeeper.Permissions, res map[string]int) map[string]int {
    res["user_count"] = 0
    users, err := zookeeper.Users("", p)
    if err != nil {
        return res
    }
    res["user_count"] = len(users)
    return res
}
*/

func Overview(w http.ResponseWriter, r *http.Request) templates.Result {
	user := r.Context().Value("user").(mw.SessionUser)
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	res := struct {
		Boxes         []OverviewItem
		Notifications []n.Notification
	}{
		append(
			brokerOverview(),
			topicOverview(ctx, user.Permissions)...),
		n.List(ctx),
	}
	return templates.DefaultRenderer("overview", res)
}
