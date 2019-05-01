package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	m "github.com/cloudkarafka/cloudkarafka-manager/metrics"
)

func ThroughputFollow(r *http.Request, quit <-chan bool) <-chan []byte {
	out := make(chan []byte, 0)
	var (
		queryParams = r.URL.Query()
		t           = queryParams["type"][0]
		id          string
	)
	if queryParams["id"] != nil {
		id = queryParams["id"][0]
	} else {
		id = ""
	}
	keys := []string{
		fmt.Sprintf("%s/%s/BytesInPerSec", t, id),
		fmt.Sprintf("%s/%s/BytesOutPerSec", t, id),
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				data := make(map[string]interface{})
				x := 0
				for _, key := range keys {
					if s, err := m.GetSerie(key); err == nil {
						l := s.Last()
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
				ticker.Stop()
				close(out)
			}
		}
	}()

	return out
}

func Throughput(r *http.Request) (interface{}, error) {
	var (
		queryParams = r.URL.Query()
		t           = queryParams["type"][0]
		id          string
	)
	if queryParams["id"] != nil {
		id = queryParams["id"][0]
	} else {
		id = ""
	}
	keys := []string{
		fmt.Sprintf("%s/%s/BytesInPerSec", t, id),
		fmt.Sprintf("%s/%s/BytesOutPerSec", t, id),
	}
	res := make([]map[string]interface{}, len(keys))
	for i, key := range keys {
		if s, err := m.GetSerie(key); err == nil {
			res[i] = map[string]interface{}{
				"name": key,
				"data": s.All(),
			}
		}
	}
	return res, nil
}
