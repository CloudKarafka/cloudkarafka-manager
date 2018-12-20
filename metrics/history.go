package metrics

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/db"
	bolt "go.etcd.io/bbolt"
)

var TopicBeans = []string{
	"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*",
	"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*",
	"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=*",
	"kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec,topic=*",
}
var BrokerBeans = []string{
	"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec",
	"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec",
	"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec",
	"kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec",
}

type TT interface {
	CreateBucketIfNotExists([]byte) (*bolt.Bucket, error)
}

func bucketByPath(tx *bolt.Tx, path string) (*bolt.Bucket, error) {
	parts := strings.Split(path, "/")
	b := TT(tx)
	var (
		err error
		b2  *bolt.Bucket
	)
	for _, r := range parts {
		if b2, err = b.CreateBucketIfNotExists([]byte(r)); err != nil {
			return nil, err
		}
		b = TT(b2)
	}
	return b2, nil
}

func FetchAndStoreMetrics(beans []string, pathFn func(Metric) string) {
	l := len(BrokerUrls) * len(beans)
	ch := make(chan []Metric)
	for brokerId, _ := range BrokerUrls {
		for _, bean := range beans {
			go QueryBrokerAsync(brokerId, bean, "OneMinuteRate", ch)
		}
	}
	timeout := time.After(5 * time.Second)
	key := time.Now().Format(time.RFC3339)
	err := db.Update(func(tx *bolt.Tx) error {
		for i := 0; i < l; i++ {
			select {
			case vs := <-ch:
				for _, v := range vs {
					path := pathFn(v)
					bucket, err := bucketByPath(tx, path)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[INFO] FetchAndStoreMetrics: Failed to create bucket for %s: %s\n", path, err)
						return err
					}
					bs := make([]byte, 8)
					binary.BigEndian.PutUint64(bs, uint64(int(v.Value)))
					err = bucket.Put([]byte(key), bs)
					if err != nil {
						return err
					}
				}
			case <-timeout:
				fmt.Fprintf(os.Stderr, "[INFO] Topic metrics request timed out\n")
				return nil
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] Failed to fetch history metrics: %s\n", err)
	}
}
