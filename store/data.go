package store

import (
	"cloudkarafka-mgmt/config"

	"sort"
	"time"
)

type Timeseries map[int64]int

func (me Timeseries) Add(val int, ts int64) Timeseries {
	if me == nil {
		me = make(map[int64]int)
	}
	var t time.Time
	if ts == 0 {
		t = time.Now()
	} else {
		t = time.Unix(ts, 0)
	}
	ts = t.Round(30 * time.Second).Unix()
	//If point already exists take average
	if v, ok := me[ts]; ok {
		me[ts] = (v + val) / 2
		return me
	}
	oldest := me.oldestKey()
	//Oldest is older than 5 minutes, delete the entry
	if oldest < time.Now().Unix()-config.Retention {
		delete(me, oldest)
	}
	me[ts] = val
	return me
}

func (me Timeseries) sortedKeys() []int {
	var (
		keys = make([]int, len(me))
		i    = 0
	)
	for k, _ := range me {
		keys[i] = int(k)
		i++
	}
	sort.Ints(keys)
	return keys
}

func (me Timeseries) oldestKey() int64 {
	keys := me.sortedKeys()
	if len(keys) == 0 {
		return 0
	}
	return int64(keys[0])
}

func (me Timeseries) Latest() int {
	keys := me.sortedKeys()
	if len(keys) > 0 {
		return me[int64(keys[len(keys)-1])]
	} else {
		return 0
	}
}
