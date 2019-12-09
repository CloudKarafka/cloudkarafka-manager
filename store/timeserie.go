package store

import (
	"fmt"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
)

type TimeSerie interface {
	Interval() int
	All() []Point
	Last() Point
	Len() int
}

type Point int

type SimpleTimeSerie struct {
	interval int
	Points   []Point
	latest   int
}

func NewSimpleTimeSerie(interval, maxPoints int) *SimpleTimeSerie {
	return &SimpleTimeSerie{
		interval: interval,
		Points:   make([]Point, maxPoints),
		latest:   -1,
	}
}

func (me *SimpleTimeSerie) Add(_x int, y int) {
	fmt.Printf("Y=%v last=%v\n", y, me.latest)
	if me.latest == -1 {
		me.latest = y
		return
	}
	v := (y - me.latest) / me.interval
	me.latest = y
	copy(me.Points, me.Points[1:])
	me.Points[me.Len()-1] = Point(v)
}
func (me *SimpleTimeSerie) Interval() int {
	return me.interval
}
func (me *SimpleTimeSerie) All() []Point {
	return me.Points[:me.Len()]
}
func (me *SimpleTimeSerie) Last() Point {
	if len(me.Points) == 0 {
		return 0
	} else {
		return me.Points[me.Len()-1]
	}
}
func (me *SimpleTimeSerie) Len() int {
	return len(me.Points)
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
	res := make([]Point, me.Len())
	for _, serie := range me.Series {
		for i, p := range serie.All() {
			res[i] += p
		}
	}
	return res
}

func (me *SumTimeSerie) Last() Point {
	all := me.All()
	l := len(all)
	if l == 0 {
		return Point(0)
	}
	return all[len(all)-1]
}

func (me *SumTimeSerie) Len() int {
	l := 0
	for _, serie := range me.Series {
		if serie.Len() > l {
			l = serie.Len()
		}
	}
	return l
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
