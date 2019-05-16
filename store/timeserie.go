package store

import (
	"fmt"
	"sort"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
)

type TimeSerie interface {
	Interval() int
	All() []Point
	Last() Point
}

type Point struct {
	X int `json:"x"`
	Y int `json:"y"`
}

type SimpleTimeSerie struct {
	interval int
	Points   []Point
	last     int
	cursor   int
}

func NewSimpleTimeSerie(interval, maxPoints int) *SimpleTimeSerie {
	return &SimpleTimeSerie{
		interval: interval,
		Points:   make([]Point, maxPoints),
		cursor:   0,
		last:     -1,
	}
}

func (me *SimpleTimeSerie) Add(x int, y int) {
	if me.last != -1 {
		v := (y - me.last) / me.interval
		p := Point{x, v}
		if me.cursor == len(me.Points)-1 {
			me.cursor = 0
		}
		me.Points[me.cursor] = p
		me.cursor += 1
	}
	me.last = y
}
func (me *SimpleTimeSerie) Interval() int {
	return me.interval
}
func (me *SimpleTimeSerie) All() []Point {
	res := make([]Point, 0)
	l := len(me.Points)
	c := me.cursor
	for i := c; i < c+l; i++ {
		p := me.Points[i%l]
		if p.X != 0 {
			res = append(res, p)
		}
	}
	return res
}
func (me *SimpleTimeSerie) Last() Point {
	if me.cursor == 0 {
		return me.Points[len(me.Points)-1]
	} else {
		return me.Points[me.cursor-1]
	}
}

type SumTimeSerie struct {
	Series []TimeSerie
}

func NewSumTimeSerie(series []TimeSerie) TimeSerie {
	return &SumTimeSerie{Series: series}
}
func (me *SumTimeSerie) Interval() int {
	return me.Series[0].Interval()
}
func (me *SumTimeSerie) All() []Point {
	all := make([]Point, 0)
	for _, serie := range me.Series {
		all = append(all, serie.All()...)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].X < all[j].X
	})
	lastX := -1
	index := -1
	interval := me.Interval()
	res := make([]Point, len(all))
	for _, p := range all {
		x := p.X / interval * interval
		if x == lastX {
			res[index].Y += p.Y
		} else {
			index += 1
			res[index] = Point{x, p.Y}
			lastX = x
		}
	}
	if index == -1 {
		return res
	}
	return res[:index]
}

func (me *SumTimeSerie) Last() Point {
	all := me.All()
	l := len(all)
	if l == 0 {
		return Point{0, 0}
	}
	return all[len(all)-1]
}

type SerieKey struct {
	Type      string
	BrokerId  int
	TopicName string
	Metric    string
}

func (sk SerieKey) String() string {
	if sk.Type == "broker" {
		return fmt.Sprintf("broker[%d] %s", sk.BrokerId, sk.Metric)
	}
	return fmt.Sprintf("broker[%d] topic=%s %s", sk.BrokerId, sk.TopicName, sk.Metric)
}

var (
	Series  = make(map[SerieKey]TimeSerie)
	quitter = make(chan bool)
)

func Subscribe(interval, maxPoints int, requests *[]MetricRequest) {
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	ch := make(chan Metric)
	for {
		select {
		case <-ticker.C:
			for _, request := range *requests {
				go func(request MetricRequest) {
					data, err := GetMetrics(request)
					if err != nil {
						log.Error("timeserie_getdata", log.ErrorEntry{err})
					} else {
						for _, metric := range data {
							ch <- metric
						}
					}
				}(request)
			}
		case metric := <-ch:
			var key SerieKey
			if metric.Topic == "" {
				key = SerieKey{"broker", metric.Broker, "", metric.Name}
			} else {
				key = SerieKey{"topic", metric.Broker, metric.Topic, metric.Name}
			}
			if _, ok := Series[key]; !ok {
				Series[key] = NewSimpleTimeSerie(interval, maxPoints)
			}
			timestamp := int(time.Now().UTC().Unix())
			Series[key].(*SimpleTimeSerie).Add(timestamp, int(metric.Value))
		}
	}
}

func GetTimeserie(key SerieKey) TimeSerie {
	return Series[key]
}

func BrokerTotal(metricName string) TimeSerie {
	series := make([]TimeSerie, 0)
	for k, v := range Series {
		if k.Type == "broker" && k.Metric == metricName {
			series = append(series, v)
		}
	}
	if len(series) == 0 {
		return nil
	}
	return NewSumTimeSerie(series)
}
func TopicTotal(topic, metricName string) TimeSerie {
	series := make([]TimeSerie, 0)
	for k, v := range Series {
		if k.Type == "topic" && k.TopicName == topic && k.Metric == metricName {
			series = append(series, v)
		}
	}
	if len(series) == 0 {
		return nil
	}
	return NewSumTimeSerie(series)
}

func StartCollect() {
	ch := make(chan map[int]config.HostPort)
	config.BrokerChangeListeners = append(config.BrokerChangeListeners, ch)
	reqs := make([]MetricRequest, 0)
	go Subscribe(5, 500, &reqs)
	for v := range ch {
		tr := make([]MetricRequest, 0)
		for id, _ := range v {
			tr = append(tr,
				MetricRequest{id, BeanAllTopicsBytesInPerSec, "Count"},
				MetricRequest{id, BeanAllTopicsBytesOutPerSec, "Count"},
				MetricRequest{id, BeanBrokerBytesInPerSec, "Count"},
				MetricRequest{id, BeanBrokerBytesOutPerSec, "Count"})
		}
		reqs = tr
	}

}
