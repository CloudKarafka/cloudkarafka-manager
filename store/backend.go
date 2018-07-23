package store

import (
	"sync"
	"time"
)

type backend struct {
	sync.RWMutex
	segments   []Store
	indices    map[string][]idx
	indexTypes map[string][]string
}

type idx struct {
	Segment int
	Pos     int
}

func newBackend() backend {
	return backend{
		segments:   make([]Store, 0),
		indices:    make(map[string][]idx),
		indexTypes: make(map[string][]string),
	}
}

func (me backend) Intersection(indexNames ...string) Store {
	me.RLock()
	defer me.RUnlock()
	seen := make(map[idx]struct{})
	seenInBoth := make(map[idx]struct{})

	for _, name := range indexNames {
		idxSet := make(map[idx]struct{})
		for _, i := range me.indices[name] {
			idxSet[i] = struct{}{}
		}
		for i, _ := range idxSet {
			if _, ok := seen[i]; ok {
				seenInBoth[i] = struct{}{}
			} else {
				seen[i] = struct{}{}
			}
		}
	}
	intersection := make([]idx, len(seenInBoth))
	i := 0
	for id, _ := range seenInBoth {
		intersection[i] = id
		i += 1
	}
	return me.subset(intersection...)
}

func (me backend) Union(indexNames ...string) Store {
	me.RLock()
	defer me.RUnlock()
	set := make(map[idx]struct{})
	for _, name := range indexNames {
		for _, i := range me.indices[name] {
			set[i] = struct{}{}
		}
	}
	union := make([]idx, len(set))
	i := 0
	for idx, _ := range set {
		union[i] = idx
		i += 1
	}
	return me.subset(union...)
}

func (me backend) SelectWithIndex(indexName string) Store {
	me.RLock()
	defer me.RUnlock()
	return me.subset(me.indices[indexName]...)
}

func (me backend) IndexedNames(name string) []string {
	me.RLock()
	defer me.RUnlock()
	return me.indexTypes[name]
}

func (me *backend) subset(indices ...idx) Store {
	me.RLock()
	defer me.RUnlock()
	selected := make(Store, len(indices))
	for i, index := range indices {
		selected[i] = me.segments[index.Segment][index.Pos]
	}
	return selected
}

func (me *backend) Put(data Data, indexOn []string) {
	me.Lock()
	defer me.Unlock()
	now := time.Now().Unix()
	if data.Timestamp == 0 || data.Timestamp > now {
		data.Timestamp = now
	}
	nrSegments := len(me.segments)
	if nrSegments == 0 || len(me.segments[nrSegments-1]) >= 1000 {
		me.segments = append(me.segments, make(Store, 0))
		nrSegments = len(me.segments)
	}
	currentSegment := me.segments[nrSegments-1]
	for _, n := range indexOn {
		index, ok := data.Tags[n]
		if !ok {
			continue
		}
		if _, ok = me.indices[index]; !ok {
			me.indexTypes[n] = append(me.indexTypes[n], index)
		}
		me.indices[index] = append(me.indices[index], idx{
			Segment: nrSegments - 1,
			Pos:     len(currentSegment),
		})
	}
	me.segments[len(me.segments)-1] = append(currentSegment, data)
}

func (me *backend) GC() time.Duration {
	length := len(me.segments)
	start := time.Now()
	if length < 2 {
		return time.Now().Sub(start)
	}
	firstSegmentToKeep := 0
	me.RLock()
	for i, s := range me.segments {
		ts := time.Unix(s[len(s)-1].Timestamp, 0)
		if time.Now().Sub(ts) < 5*time.Minute {
			firstSegmentToKeep = i
			break
		}
	}
	me.RUnlock()
	if firstSegmentToKeep == 0 {
		return time.Now().Sub(start)
	}
	segments := me.segments[firstSegmentToKeep:]
	indicesToKeep := make(map[string][]idx)
	indexTypeToKeep := make(map[string][]string)
	me.RLock()
	for key, idxs := range me.indices {
		var idxsToKeep []idx
		for _, i := range idxs {
			if i.Segment > firstSegmentToKeep {
				idxsToKeep = append(idxsToKeep, idx{
					Segment: i.Segment - firstSegmentToKeep,
					Pos:     i.Pos,
				})
			}
		}
		if len(idxsToKeep) > 0 {
			indicesToKeep[key] = idxsToKeep
		}
	}
	for key, types := range me.indexTypes {
		var typesToKeep []string
		for _, t := range types {
			if idxs, ok := indicesToKeep[t]; !ok && len(idxs) > 0 {
				typesToKeep = append(typesToKeep, t)
			}
		}
		if len(typesToKeep) > 0 {
			indexTypeToKeep[key] = typesToKeep
		}
	}
	me.RUnlock()
	me.Lock()
	me.indexTypes = indexTypeToKeep
	me.indices = indicesToKeep
	me.segments = segments
	me.Unlock()
	return time.Now().Sub(start)
}

func (me *backend) Stats() (int, int, int, int) {
	size := 0
	for _, s := range me.segments {
		size += len(s)
	}
	return size, len(me.segments), len(me.indices), len(me.indexTypes)
}
