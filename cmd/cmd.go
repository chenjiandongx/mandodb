package main

import (
	"github.com/chenjiandongx/mandodb/storage"
	"time"
)

func main() {
	store := storage.NewMemorySegment()

	now := time.Now().Unix()
	for i := 1; i <= 121; i++ {
		store.InsertRow(&storage.Row{
			Metric:    "cpu.busy",
			Labels:    []storage.Label{{"core", "1"}},
			DataPoint: storage.DataPoint{Ts: now, Value: float64(i)},
		})
		now += 5
	}

	store.QueryRange("my-metric10", []storage.Label{{"a", "1"}}, 0, 100)
	//store.QueryRange("my-metric11", []Label{{"a", "2"}}, 0, 100)
}
