package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/store"
)

func seriesFromRequest(r *http.Request) (map[string]store.TimeSerie, error) {
	var queryParams = r.URL.Query()
	if len(queryParams["type"]) == 0 {
		return nil, errors.New("Bad request, no type given")
	}
	t := queryParams["type"][0]
	if t == "broker" {
		if len(queryParams["brokerid"]) == 0 {
			return nil, errors.New("Bad request, no brokerid given")
		}
		id, err := strconv.Atoi(queryParams["brokerid"][0])
		if err != nil {
			return nil, errors.New("Bad request, brokerid must be an integer")
		}
		return map[string]store.TimeSerie{
			"Bytes out": store.GetTimeserie(store.SerieKey{Type: "broker", BrokerId: id, Metric: "BytesOutPerSec"}),
			"Bytes in":  store.GetTimeserie(store.SerieKey{Type: "broker", BrokerId: id, Metric: "BytesInPerSec"}),
		}, nil
	} else if t == "topic" {
		if len(queryParams["topic"]) == 0 {
			return nil, errors.New("Bad request, no topic given")
		}
		topic := queryParams["topic"][0]
		return map[string]store.TimeSerie{
			"Bytes in":  store.TopicTotal(topic, "BytesInPerSec"),
			"Bytes out": store.TopicTotal(topic, "BytesOutPerSec"),
		}, nil
	} else if t == "total" {
		return map[string]store.TimeSerie{
			"Bytes in":  store.BrokerTotal("BytesInPerSec"),
			"Bytes out": store.BrokerTotal("BytesOutPerSec"),
		}, nil
	}
	return nil, fmt.Errorf("No throughput for %s", t)
}

func ThroughputFollow(r *http.Request, quit <-chan bool) (<-chan []byte, error) {
	out := make(chan []byte, 0)
	series, err := seriesFromRequest(r)
	if err != nil {
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				data := make(map[string]interface{})
				x := 0
				for key, serie := range series {
					if serie != nil {
						l := serie.Last()
						data[key] = l.Y
						x = l.X
					}
				}
				res := map[string]interface{}{
					"x":    x,
					"data": data,
				}
				if j, err := json.Marshal(res); err == nil {
					out <- j
				}
			case <-quit:
				close(out)
				ticker.Stop()
				return
			}
		}
	}()
	return out, nil
}

func Throughput(r *http.Request) (interface{}, error) {
	series, err := seriesFromRequest(r)
	if err != nil {
		return nil, err
	}
	res := make([]map[string]interface{}, len(series))
	i := 0
	for key, serie := range series {
		if serie != nil {
			res[i] = map[string]interface{}{
				"name": key,
				"data": serie.All(),
			}
			i += 1
		}
	}
	return res, nil
}
