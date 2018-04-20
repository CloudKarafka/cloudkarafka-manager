package dm

import (
	"sort"
)

type DataPoint struct {
	X int64 `json:"x"`
	Y int   `json:"y"`
}

type Series []DataPoint

func (s Series) Len() int      { return len(s) }
func (s Series) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s Series) Less(i, j int) bool {
	return s[i].X < s[j].X
}

func (s *Series) Sort() {
	sort.Sort(s)
}
