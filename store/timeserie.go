package store

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
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

type BeanTimeSerie struct {
	interval  int
	Points    []Point
	listeners []chan Point
	last      int
	cursor    int
}

func NewBeanTimeSerie(r MetricRequest, interval, maxPoints int) *BeanTimeSerie {
	ts := &BeanTimeSerie{
		interval: interval,
		Points:   make([]Point, maxPoints),
		cursor:   0,
		last:     -1,
	}
	go ts.fromBean(r)
	return ts
}
func (me *BeanTimeSerie) add(x int, y int) {
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
func (me *BeanTimeSerie) Interval() int {
	return me.interval
}
func (me *BeanTimeSerie) All() []Point {
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
func (me *BeanTimeSerie) Last() Point {
	if me.cursor == 0 {
		return me.Points[len(me.Points)-1]
	} else {
		return me.Points[me.cursor-1]
	}
}

func (me *BeanTimeSerie) fromBean(r MetricRequest) {
	for {
		time.Sleep(time.Duration(me.interval) * time.Second)
		metrics, err := QueryBroker(r)
		if err != nil {
			log.Error("timeserie_getdata", log.ErrorEntry{err})
		} else {
			timestamp := int(time.Now().UTC().Unix())
			for _, metric := range metrics {
				me.add(timestamp, int(metric.Value))
			}
		}
	}
}

type SumTimeSerie struct {
	Series []TimeSerie
}

func NewSumTimeSerie(series []TimeSerie) *SumTimeSerie {
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

var (
	Series = make(map[string]TimeSerie)
)

func ensureExists(key string) error {
	parts := strings.Split(key, "/")
	if _, ok := Series[key]; !ok || parts[0] == "total" {
		if parts[0] == "broker" {
			id, _ := strconv.Atoi(parts[1])
			Series[key] = NewBeanTimeSerie(BrokerBeanRequest(id, parts[2]), 5, 500)
		} else if parts[0] == "total" {
			for id, _ := range config.BrokerUrls {
				ensureExists(fmt.Sprintf("broker/%d/BytesInPerSec", id))
				ensureExists(fmt.Sprintf("broker/%d/BytesOutPerSec", id))
			}
			series := make([]TimeSerie, 0)
			for k, v := range Series {
				if strings.HasPrefix(k, "broker/") && strings.HasSuffix(k, parts[2]) {
					series = append(series, v)
				}
			}
			if len(series) == 0 {
				return errors.New("No brokers to sum throughput for")
			}
			Series[key] = NewSumTimeSerie(series)
		} else if parts[0] == "topic" {
			series := make([]TimeSerie, 0)
			for id, _ := range config.BrokerUrls {
				series = append(series, NewBeanTimeSerie(TopicBeanRequest(id, parts[1], parts[2]), 5, 200))
			}
			Series[key] = NewSumTimeSerie(series)
		}
	}
	return nil
}

func GetSerie(key string) (TimeSerie, error) {
	if err := ensureExists(key); err != nil {
		return nil, err
	}
	return Series[key], nil
}
