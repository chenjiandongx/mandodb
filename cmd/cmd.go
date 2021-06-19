package main

import (
	"time"

	"github.com/chenjiandongx/mandodb/storage"
)

func main() {
	store := storage.OpenTSDB()

	now := time.Now().Unix()

	for i := 1; i <= 150; i++ {
		store.InsertRow(&storage.Row{
			Metric:    "cpu.busy",
			Labels:    []storage.Label{{"core", "1"}},
			DataPoint: storage.DataPoint{Ts: now, Value: float64(i)},
		})
		now += 10
	}

	//fmt.Println(now-(5*160), now-(5*150))
	store.QueryRange("cpu.busy", nil, now-(5*125), now-(5*20))
}
