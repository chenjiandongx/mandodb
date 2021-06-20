package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/chenjiandongx/mandodb/storage"
)

func main() {
	store := storage.OpenTSDB()

	now := time.Now().Unix()

	for i := 1; i <= 150; i++ {
		for j := 0; j < 12; j++ {
			for k := 0; k < 64; k++ {
				store.InsertRow(&storage.Row{
					Metric: "cpu.busy",
					Labels: []storage.Label{
						{Name: "core", Value: strconv.Itoa(k)},
						{Name: "node", Value: "vm" + strconv.Itoa(j)},
					},
					DataPoint: storage.DataPoint{Ts: now, Value: float64(i)},
				})
			}
		}

		now += 65
	}

	fmt.Println(now-(5*100), now-(5*20))
	store.QueryRange(
		"cpu.busy",
		[]storage.Label{
			//{Name: "node", Value: "vm1"},
			{Name: "core", Value: "12"},
		},
		now-(5*100),
		now-(5*20),
	)
}
