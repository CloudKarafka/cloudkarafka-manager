package store

import (
	"fmt"
	"time"
)

type sweeper struct {
	running bool
}

type puttable struct {
	D          Data
	IndexNames []string
}

func (s sweeper) Start() {
	s.running = true
	for {
		fmt.Printf("[INFO] sweeper-start store-size=%v\n", Store.Len())
		lock.Lock()
		s := time.Now()
		old := time.Now().UTC().Add(-10 * time.Minute)
		keep := make(map[string]puttable)
		for t, names := range Store.IndexTypes() {
			for _, name := range names {
				ints := Store.Index(name)
				for _, i := range ints {
					d := Store.Stored[i]
					if time.Unix(d.Timestamp, 0).Before(old) {
						fmt.Println(d.Timestamp)
						continue
					}
					if _, ok := keep[d.Key()]; !ok {
						keep[d.Key()] = puttable{D: d}
					}
					k := keep[d.Key()]
					k.IndexNames = append(k.IndexNames, t)
					keep[d.Key()] = k
				}
			}
		}
		fmt.Printf("[INFO] sweeper-completed time-spent=%v len-before=%v len-after=%v\n",
			time.Since(s), Store.Len(), len(keep))
		Store.Copy(keep)
		lock.Unlock()
		time.Sleep(15 * time.Second)
	}
}

func init() {
	//go sweeper{running: false}.Start()
}
