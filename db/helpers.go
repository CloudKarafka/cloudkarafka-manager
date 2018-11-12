package db

import (
	"bytes"
	"encoding/gob"
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

func s(buf []byte) (string, error) {
	return bytes.NewBuffer(buf).String(), nil
}

func f(buf []byte) (float64, error) {
	dec := gob.NewDecoder(bytes.NewBuffer(buf))
	var q float64
	err := dec.Decode(&q)
	if err != nil {
		return q, err
	}
	return q, nil
}
func i(buf []byte) (interface{}, error) {
	var (
		v   interface{}
		err error
	)
	if v, err = f(buf); err == nil {
		return v, nil
	} else if v, err := s(buf); err == nil {
		return v, nil

	}
	return nil, err
}

func GetFloat(b *bolt.Bucket, key string) float64 {
	v := b.Get([]byte(key))
	vv, _ := f(v)
	return vv
}

// Get the value that is last in a bucket, the bucket must be key sorted (for example dates)
func Last(b *bolt.Bucket) interface{} {
	c := b.Cursor()
	_, v := c.Last()
	if tv, err := i(v); err == nil {
		return tv
	}
	return nil
}

// Reads only the key=values that aren't sub-buckets
func Values(b *bolt.Bucket) map[string]interface{} {
	c := b.Cursor()
	res := make(map[string]interface{})
	for k, v := c.First(); k != nil; k, v = c.Next() {
		key := string(k)
		if v != nil {
			if tv, err := i(v); err == nil {
				res[key] = tv
			}
		}
	}
	return res
}

func Dig(base *bolt.Bucket, depth int, cb func(map[string]interface{})) {
	if depth == 0 {
		cb(Values(base))
	}
	c := base.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v == nil {
			if next := base.Bucket(k); next != nil {
				Dig(next, depth-1, cb)
			}
		}
	}
}

func Recur(b *bolt.Bucket, depth int) map[string]interface{} {
	c := b.Cursor()
	res := make(map[string]interface{})
	for k, v := c.First(); k != nil; k, v = c.Next() {
		key := string(k)
		if v == nil && depth > 0 {
			res[key] = Recur(b.Bucket(k), depth-1)
		} else {
			if tv, err := i(v); err == nil {
				res[key] = tv
			}
		}
	}
	return res
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
		tv, e := f(v)
		if e == nil && err == nil {
			res = append(res, DataPoint{t.Unix(), int64(tv)})
		}
	}
	return res
}
