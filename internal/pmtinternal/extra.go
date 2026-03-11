package pmtinternal

import (
	"runtime"
	"sync"
)

type FlushExtraParams struct {
	Low  uint64
	High uint64
}

// FlushExtraParamsByGoID stores per-goroutine extra params for passing data
// without changing function signatures.
var FlushExtraParamsByGoID sync.Map // map[uint64]FlushExtraParams
var CollectorDoneByGoID sync.Map    // map[uint64]bool

func currentGoID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	if n <= len(prefix) {
		panic("goid")
	}
	var id uint64
	for i := len(prefix); i < n; i++ {
		c := buf[i]
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + uint64(c-'0')
	}
	if id == 0 {
		panic("goid")
	}
	return id
}

func SetFlushExtraParams(low uint64, high uint64) {
	FlushExtraParamsByGoID.Store(currentGoID(), FlushExtraParams{
		Low:  low,
		High: high,
	})
}

func GetFlushExtraParams() (low uint64, high uint64, ok bool) {
	v, ok := FlushExtraParamsByGoID.Load(currentGoID())
	if !ok {
		return 0, 0, false
	}
	p, ok := v.(FlushExtraParams)
	if !ok {
		panic("flush extra params type")
	}
	return p.Low, p.High, true
}

func GetAndDelFlushExtraParams() (low uint64, high uint64) {
	v, ok := FlushExtraParamsByGoID.LoadAndDelete(currentGoID())
	if !ok {
		panic("ExtraParams !ok")
	}
	p, ok := v.(FlushExtraParams)
	if !ok {
		panic("flush extra params type")
	}
	return p.Low, p.High
}

func ClearFlushExtraParams() {
	FlushExtraParamsByGoID.Delete(currentGoID())
}

func SetCollectorDone(done bool) {
	CollectorDoneByGoID.Store(currentGoID(), done)
}

func GetAndDelCollectorDone() (done bool, ok bool) {
	v, ok := CollectorDoneByGoID.LoadAndDelete(currentGoID())
	if !ok {
		return false, false
	}
	done, ok = v.(bool)
	if !ok {
		panic("collector done type")
	}
	return done, true
}
