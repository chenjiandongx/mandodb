package main

import (
	"fmt"
	"github.com/chenjiandongx/mandodb/storage"
	"time"
)

func main() {
	store := storage.OpenTSDB()
	defer store.Close()

	//var now int64 = 1

	// 1108992 > 100w
	//for i := 1; i <= 1444; i++ { // 2h
	//	for j := 0; j < 12; j++ { //
	//		for k := 0; k < 64; k++ {
	//			store.InsertRow(&storage.Row{
	//				Metric: "cpu.busy",
	//				Labels: []storage.Label{
	//					{Name: "core", Value: strconv.Itoa(k)},
	//					{Name: "node", Value: "vm" + strconv.Itoa(j)},
	//				},
	//				DataPoint: storage.DataPoint{Ts: now, Value: float64(i)},
	//			})
	//		}
	//	}
	//
	//	now += 5
	//}

	t0 := time.Now()
	store.QueryRange(
		"cpu.busy",
		[]storage.Label{
			{Name: "node", Value: "vm1"},
			{Name: "core", Value: "12"},
		},
		0,
		50,
	)
	fmt.Println("take:", time.Since(t0))
}
