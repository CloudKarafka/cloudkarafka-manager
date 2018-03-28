package store

import (
	"fmt"
	"time"
)

type sweeper struct {
	running bool
}

type sw struct {
	D          Data
	IndexNames []string
}

func (s sweeper) Start() {
	s.running = true
	for {
		l.Lock()
		fmt.Printf("[INFO] sweeper-start store-size=%v\n", len(Store))
		s := time.Now()
		keep := make(map[string]sw)
		for indexName, ints := range indexes {
			for _, i := range ints {
				d := Store[i]
				if time.Unix(d.Timestamp, 0).Before(time.Now().Add(-1 * time.Minute)) {
					continue
				}
				if _, ok := keep[d.Key()]; !ok {
					keep[d.Key()] = sw{D: d}
				}
				k := keep[d.Key()]
				k.IndexNames = append(k.IndexNames, indexName)
				keep[d.Key()] = k
			}
		}
		if len(keep) != len(Store) {
			resetStore()
			l.Unlock()
			for _, d := range keep {
				Put(d.D, d.IndexNames)
			}
		} else {
			l.Unlock()
		}
		fmt.Printf("[INFO] sweeper-completed time-spent=%v\n", time.Since(s))
		time.Sleep(15 * time.Second)
	}
}

func resetStore() {
	indexes = make(map[string][]int)
	indexedNames = make(map[string][]string)
	Store = make(store, 0)
}

func init() {
	go sweeper{}.Start()
}
