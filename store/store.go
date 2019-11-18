package store

import (
	"strings"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func adminClient() (*kafka.AdminClient, error) {
	adminConfig := &kafka.ConfigMap{"bootstrap.servers": strings.Join(config.BrokerUrls.List(), ",")}
	return kafka.NewAdminClient(adminConfig)
}
