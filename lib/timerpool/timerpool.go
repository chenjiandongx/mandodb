package timerpool

import (
	"sync"
	"time"
)

var timerPool sync.Pool

// Get 从资源池获取一个 Timer
func Get(d time.Duration) *time.Timer {
	if v := timerPool.Get(); v != nil {
		t := v.(*time.Timer)
		if t.Reset(d) {
			panic("active timer trapped to the pool!")
		}
		return t
	}
	return time.NewTimer(d)
}

// Put 将 Timer 放入资源池
func Put(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C: // 确保 channel 最后被消费
		default:
		}
	}
	timerPool.Put(t)
}
