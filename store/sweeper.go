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
		fmt.Printf("[INFO] sweeper-start store-size=%v\n", Store.Len())
		s := time.Now()
		keep := make(map[string]sw)
		Store.RLock()
		for indexName, ints := range Store.Indexes() {
			for _, i := range ints {
				d := Store.Stored[i]
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
		Store.RUnlock()
		newStore := newStore(0)
		for _, k := range keep {
			newStore.Put(k.D, k.IndexNames)
		}
		fmt.Printf("[INFO] sweeper-completed time-spent=%v len-before=%v len-after=%v\n",
			time.Since(s), Store.Len(), newStore.Len())
		Store = newStore
		time.Sleep(15 * time.Second)
	}
}

func init() {
	go sweeper{running: false}.Start()
}
