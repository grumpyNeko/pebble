package pmtinternal

import (
	"runtime"
	"sync"
)

type FlushExtraParams struct {
	Low  uint64
	High uint64
}

type flushExtraParamsByGoID struct {
	byGoID sync.Map
}

func (e *flushExtraParamsByGoID) Set(low uint64, high uint64) {
	e.byGoID.Store(currentGoID(), FlushExtraParams{
		Low:  low,
		High: high,
	})
}

func (e *flushExtraParamsByGoID) Get() (low uint64, high uint64, ok bool) {
	v, ok := e.byGoID.Load(currentGoID())
	if !ok {
		return 0, 0, false
	}
	p, ok := v.(FlushExtraParams)
	if !ok {
		panic("flush extra params type")
	}
	return p.Low, p.High, true
}

func (e *flushExtraParamsByGoID) GetAndDel() (low uint64, high uint64) {
	v, ok := e.byGoID.LoadAndDelete(currentGoID())
	if !ok {
		panic("flush extra params !ok")
	}
	p, ok := v.(FlushExtraParams)
	if !ok {
		panic("flush extra params type")
	}
	return p.Low, p.High
}

func (e *flushExtraParamsByGoID) Clear() {
	e.byGoID.Delete(currentGoID())
}

type collectorDoneByGoID struct {
	byGoID sync.Map
}

func (e *collectorDoneByGoID) Set(done bool) {
	e.byGoID.Store(currentGoID(), done)
}

func (e *collectorDoneByGoID) Get() (done bool, ok bool) {
	v, ok := e.byGoID.Load(currentGoID())
	if !ok {
		return false, false
	}
	done, ok = v.(bool)
	if !ok {
		panic("collector done type")
	}
	return done, true
}

func (e *collectorDoneByGoID) GetAndDel() bool {
	v, ok := e.byGoID.LoadAndDelete(currentGoID())
	if !ok {
		panic("collector done !ok")
	}
	done, ok := v.(bool)
	if !ok {
		panic("collector done type")
	}
	return done
}

func (e *collectorDoneByGoID) Clear() {
	e.byGoID.Delete(currentGoID())
}

var ExtraParam = struct {
	FlushExtraParams flushExtraParamsByGoID
	CollectorDone    collectorDoneByGoID
}{}

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
