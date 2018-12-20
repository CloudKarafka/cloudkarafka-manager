package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

func BucketByPath(tx *bolt.Tx, path string) *bolt.Bucket {
	parts := strings.Split(path, "/")
	var b *bolt.Bucket = tx.Bucket([]byte(parts[0]))
	if b == nil {
		return nil
	}
	for _, p := range parts[1:] {
		b = b.Bucket([]byte(p))
		if b == nil {
			fmt.Fprintf(os.Stderr, "[WARN] db.BucketByPath: No bucket for '%s'\n", path)
			return nil
		}
	}
	return b
}

type DataPoint struct {
	X int64 `json:"x"`
	Y int64 `json:"y"`
}

type Serie struct {
	Name string
	Type string
	Data []DataPoint
}

func (s *Serie) Add(dp []DataPoint) {
	s.Data = append(s.Data, dp...)
}

func (s *Serie) dataPoints() []DataPoint {
	sort.Slice(s.Data, func(i, j int) bool {
		return s.Data[i].X < s.Data[j].X
	})
	var lastX int64 = -1
	index := 0
	res := make([]DataPoint, len(s.Data))
	for _, r := range s.Data {
		x := r.X / 30 * 30
		if x == lastX {
			res[index].Y += r.Y
		} else {
			res[index] = DataPoint{x, r.Y}
			lastX = x
			index += 1
		}
	}
	return res[:index]
}
func (s *Serie) MarshalJSON() ([]byte, error) {
	res := map[string]interface{}{
		"name": s.Name,
		"type": s.Type,
		"data": s.dataPoints(),
	}
	return json.Marshal(res)
}

func TimeSerie(tx *bolt.Tx, path string, from time.Time) []DataPoint {
	min := []byte(from.Format(time.RFC3339))
	max := []byte(time.Now().Format(time.RFC3339))
	var res []DataPoint
	b := BucketByPath(tx, path)
	if b == nil {
		return nil
	}
	c := b.Cursor()
	for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
		t, err := time.Parse(time.RFC3339, string(k))
		if err == nil {
			res = append(res, DataPoint{t.Unix(), int64(binary.BigEndian.Uint64(v))})
		}
	}
	return res
}
