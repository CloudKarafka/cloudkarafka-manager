package store

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

type t struct {
	Name string
}

func tairStore() {
	indexes = make(map[string][]int)
	Store = make([]Data, 0)
}

func seedStore(elems int) {
	for i := 0; i < elems; i++ {
		var st storageType
		if i%2 == 0 {
			st = JMX
		} else {
			st = ConsumerOffset
		}
		data := Data{
			Id: map[string]string{
				"Name": fmt.Sprintf("test%v", i),
				"Type": st.String(),
			},
			Value: 10,
		}
		Put(data, []string{"Name", "Type"})
	}
}

func TestMain(m *testing.M) {
	seedStore(10000)
	os.Exit(m.Run())
}

func TestIndexedNames(test *testing.T) {
	names := IndexedNames("Type")
	if len(names) != 2 {
		test.Error("Expected 2 types got", len(names))
	}
}

func TestIntersection(test *testing.T) {
	inter := Intersection("test0", JMX.String())
	if len(inter) != 1 {
		test.Error("Expected 1 stored element got", len(inter))
	}
}

func TestSelectAllOfType(test *testing.T) {
	selected := Store.Select(func(data Data) bool {
		return data.Id["Type"] == JMX.String()
	})
	if len(selected) != 5000 {
		test.Error("Expected 5000 stored element got", len(selected))
	}
}

func TestFind(test *testing.T) {
	data, err := Store.Find(func(d Data) bool {
		return d.Id["Name"] == "test0"
	})
	if err != nil {
		test.Error("Expected test0 found nothing")
	}
	if data.Id["Name"] != "test0" {
		test.Error("Expected test0 got", data.Id["Name"])
	}
}

func benchmarkStoreSelect(elems int, b *testing.B) {
	seedStore(elems)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Store.Select(func(d Data) bool {
			return strings.HasPrefix(d.Id["Name"], fmt.Sprintf("test%v", n))
		})
	}
	b.StopTimer()
	tairStore()
}

func BenchmarkStoreSelect1(b *testing.B)     { benchmarkStoreSelect(1, b) }
func BenchmarkStoreSelect10(b *testing.B)    { benchmarkStoreSelect(10, b) }
func BenchmarkStoreSelect100(b *testing.B)   { benchmarkStoreSelect(100, b) }
func BenchmarkStoreSelect1000(b *testing.B)  { benchmarkStoreSelect(1000, b) }
func BenchmarkStoreSelect10000(b *testing.B) { benchmarkStoreSelect(10000, b) }

func benchmarkStoreSelectWithIndex(elems int, b *testing.B) {
	seedStore(elems)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		SelectWithIndex(fmt.Sprintf("test%v", n))
	}
	b.StopTimer()
	tairStore()
}

func BenchmarkStoreSelectWithIndex1(b *testing.B)     { benchmarkStoreSelectWithIndex(1, b) }
func BenchmarkStoreSelectWithIndex10(b *testing.B)    { benchmarkStoreSelectWithIndex(10, b) }
func BenchmarkStoreSelectWithIndex100(b *testing.B)   { benchmarkStoreSelectWithIndex(100, b) }
func BenchmarkStoreSelectWithIndex1000(b *testing.B)  { benchmarkStoreSelectWithIndex(1000, b) }
func BenchmarkStoreSelectWithIndex10000(b *testing.B) { benchmarkStoreSelectWithIndex(10000, b) }

func benchmarkStoreIntersection(elems int, b *testing.B) {
	seedStore(elems)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Intersection(fmt.Sprintf("test%v", n), "1")
	}
	b.StopTimer()
	tairStore()
}

func BenchmarkStoreIntersection1(b *testing.B)     { benchmarkStoreIntersection(1, b) }
func BenchmarkStoreIntersection10(b *testing.B)    { benchmarkStoreIntersection(10, b) }
func BenchmarkStoreIntersection100(b *testing.B)   { benchmarkStoreIntersection(100, b) }
func BenchmarkStoreIntersection1000(b *testing.B)  { benchmarkStoreIntersection(1000, b) }
func BenchmarkStoreIntersection10000(b *testing.B) { benchmarkStoreIntersection(10000, b) }
