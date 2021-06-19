package main

import (
	"fmt"
	"github.com/chenjiandongx/mandodb/storage"
	"time"
)

func main() {
	store := storage.OpenTSDB()

	now := time.Now().Unix()


	for i := 1; i <= 250; i++ {
		store.InsertRow(&storage.Row{
			Metric:    "cpu.busy",
			Labels:    []storage.Label{{"core", "1"}},
			DataPoint: storage.DataPoint{Ts: now, Value: float64(i)},
		})
		now += 5
	}

	fmt.Println(now-(5*160), now-(5*150))
	store.QueryRange("cpu.busy", []storage.Label{{"a", "1"}}, now-(5*160), now-(5*150))
}
