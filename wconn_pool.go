package wconnpool

import (
	"fmt"
	"sync"
	"time"
)

var (
	NilConn = ConnWarp{}
	NilConnError = fmt.Errorf("Nil Connection")
)

type Conn interface {
	Close() error
	AliveCheck() bool
}

type CreateConnFunctor func(string) (Conn, error)
type FuseAlert func(host string, key string, msg string)

type ConnWarp struct {
	conn Conn
	deadLine time.Time
	addr string
	endPoint *EndPointInfo
}

func IsNil(conn ConnWarp) bool {
	return conn == NilConn
}

func (cw *ConnWarp) Init(conn Conn, addr string, ttl time.Duration) {
	cw.conn = conn
	cw.addr = addr
}

func (cw *ConnWarp) Close() (err error) {
	err = cw.conn.Close()
	cw.conn = nil
	return
}

func (cw *ConnWarp) CheckTtl() bool {
	if cw.deadLine.Before(time.Now()) {
		cw.conn.Close()
		return false
	}
	return true
}

func (cw *ConnWarp) IsValid() bool {
	if !cw.CheckTtl(){
		return false
	}
	return cw.conn.AliveCheck()
}

func (cw *ConnWarp) IncrSuc(key string) {
	cw.endPoint.IncrSuc(key)
}

func (cw *ConnWarp) IncrFail(key string) {
	cw.IncrFail(key)
}

type StateInfo struct {
	ep *EndPointInfo
	key string
	curTime   int64
	sucCount  uint32
	failCount uint32
	curLimit  uint32
	maxFreqLimit  uint32
	minFreqLimit  uint32
	fuseRate  float64
	fuseAlert FuseAlert
}

func (si *StateInfo) Init(ep *EndPointInfo, key string, maxFreqLimitPerSecond uint32, minFreqLimitPerSecond uint32, fuseRate float64) {
	si.key = key
	si.ep = ep
	si.maxFreqLimit = maxFreqLimitPerSecond
	si.curTime = 0
	si.sucCount = 0
	si.failCount = 0
	si.curLimit = maxFreqLimitPerSecond
	si.maxFreqLimit = si.curLimit
	si.fuseRate = fuseRate
	si.minFreqLimit = minFreqLimitPerSecond
	si.curLimit = si.maxFreqLimit
}
func (si *StateInfo) IncrSuc() {
	if time.Now().Unix() != si.curTime {
		si.curTime = time.Now().Unix()
		si.curTime = 0
		si.sucCount = 0
		si.failCount = 0
	}
	si.sucCount += 1
	if (si.sucCount + si.failCount) >= 2 && float64(si.sucCount*1.0) / float64(si.sucCount + si.failCount) > si.fuseRate {
		if si.curLimit * 2 <= si.maxFreqLimit {
			si.curLimit = si.curLimit * 2
		} else {
			si.curLimit = si.maxFreqLimit
		}
	}
}

func (si *StateInfo)Info() {
	fmt.Printf("maxLimit: %v, minLimit: %v, currentLimit:%v, sucCount:%v, failCount:%v\n",
		si.maxFreqLimit, si.minFreqLimit, si.curLimit, si.sucCount, si.failCount)
}

func (si *StateInfo) IncrFail() {
	if time.Now().Unix() != si.curTime {
		si.curTime = time.Now().Unix()
		si.sucCount = 0
		si.failCount = 0
	}
	si.failCount += 1
	if (si.sucCount + si.failCount) > 2 && (float64(si.sucCount)*1.0) / float64(si.sucCount + si.failCount) < si.fuseRate {
		var host string
		if si.ep != nil {
			host = si.ep.name
		}

		if si.curLimit / 2.0 >= si.minFreqLimit {
			si.curLimit = si.curLimit / 2.0
		} else {
			si.curLimit = si.minFreqLimit
		}
		if si.fuseAlert != nil {
			si.fuseAlert(host, si.key, fmt.Sprintf("current limit: %d", si.curLimit))
		} else{
			fmt.Printf("[FUSE] current limit: %d\n", si.curLimit)
		}
	}
}

func (si *StateInfo) reachLimit() bool {
	if time.Now().Unix() != si.curTime {
		si.curTime = time.Now().Unix()
		si.sucCount = 0
		si.failCount = 0
	}
	return si.sucCount + si.failCount < si.curLimit
}

type EndPointInfo struct {
	name string
	maxConnNum uint32
	curConnNum uint32
	states map[string]StateInfo
	maxFreqLimit  uint32
	minFreqLimit  uint32
	fuseRate  float64
	conns chan ConnWarp
	createFunctor CreateConnFunctor
	maxIdleTime time.Duration
	checkInterval time.Duration
	m sync.Mutex
}

func (ep *EndPointInfo) Init(host string, maxConnNum uint32, maxFreqLimit  uint32, minFreqLimit  uint32, fuseRate  float64, maxIdleTime time.Duration, aliveCheckInterval time.Duration, functor CreateConnFunctor) {
	ep.name = host
	ep.maxConnNum = maxConnNum
	ep.curConnNum = 0
	ep.maxFreqLimit = maxFreqLimit
	ep.minFreqLimit = minFreqLimit
	ep.fuseRate = fuseRate
	ep.maxIdleTime = maxIdleTime
	ep.conns = make(chan ConnWarp, maxConnNum)
	ep.checkInterval = aliveCheckInterval
	ep.createFunctor = functor

	go func() {
		ep.checkAliveConn()
	}()
}

func (ep *EndPointInfo) createConn() (ConnWarp, error) {
	conn, err := ep.createFunctor(ep.name)
	if err != nil {
		return ConnWarp{}, err
	}
	ep.curConnNum += 1

	cw := ConnWarp{conn: conn, deadLine: time.Now().Add(ep.maxIdleTime), endPoint: ep}
	return cw, nil
}

func (ep *EndPointInfo) Close() {
	close(ep.conns)
}

func (ep *EndPointInfo) updateConnTTl(conn ConnWarp) {
	conn.deadLine = time.Now().Add(ep.maxIdleTime)
}

func (ep *EndPointInfo) getConn(key string, waitTime time.Duration) (ConnWarp, error) {
	select {
	case conn := <-ep.conns:
		ep.updateConnTTl(conn)
		return conn, nil
	default:
		if ep.curConnNum < ep.maxConnNum {
			return ep.createConn()
		}
	}
	select {
	case conn := <-ep.conns:
		ep.updateConnTTl(conn)
		return conn, nil
	case <- time.After(waitTime):
		return NilConn, NilConnError
	}
}

func (ep *EndPointInfo) GetConn(key string, waitTime time.Duration) (ConnWarp, error) {
	for {
		if ep.reachLimit(key){
			return NilConn, NilConnError
		}
		conn, err := ep.getConn(key, waitTime)
		if err != nil {
			return NilConn, NilConnError
		}
		if conn != NilConn && err == nil && conn.CheckTtl() {
			continue
		}
		return conn, nil
	}
}

func (ep *EndPointInfo) putConn(conn ConnWarp) {
	if conn.CheckTtl() {
		ep.conns <- conn
	}
}

func (ep *EndPointInfo) checkAliveConn() {
	for {
		<- time.After(ep.checkInterval)
		select {
		case conn := <-ep.conns:
			if !conn.CheckTtl() {
				ep.conns <- conn
			} else {
				conn.Close()
			}
		default:
			break
		}
	}
}

func (ep *EndPointInfo) reachLimit(key string) bool {
	state, ok := ep.states[key]
	if ok {
		return state.reachLimit()
	}
	state = StateInfo{}
	state.Init(ep, key, ep.maxFreqLimit, ep.minFreqLimit, ep.fuseRate)
	ep.states[key] = state
	return state.reachLimit()
}

func (ep *EndPointInfo) IncrSuc(key string){
	ep.m.Lock()
	defer ep.m.Unlock()
	state, ok := ep.states[key]
	if !ok {
		state = StateInfo{}
		state.Init(ep, key, ep.maxFreqLimit, ep.minFreqLimit, ep.fuseRate)
	}
	state.IncrSuc()
	ep.states[key] = state
	//return state.reachLimit()
}

func (ep *EndPointInfo) IncrFail(key string){
	ep.m.Lock()
	defer ep.m.Unlock()
	state, ok := ep.states[key]
	if !ok {
		state = StateInfo{}
		state.ep = ep
		state.Init(ep, key, ep.maxFreqLimit, ep.minFreqLimit, ep.fuseRate)
	}
	state.IncrFail()
	ep.states[key] = state
	//return state.reachLimit()
}

type WConnPool struct {
	maxIdletime time.Time
	maxConnNum uint32
	functor CreateConnFunctor
	endpoints map[string]EndPointInfo
	m sync.Mutex
	cursor uint32
}

const(
	defaultMaxIdleTime time.Duration = time.Minute
	defaultAliveCheckInterval time.Duration = time.Second
	defaultMinFreqLimit uint32 = 1
)

func (wp *WConnPool) Init(maxConnNum uint32, maxIdletime time.Time, functor CreateConnFunctor) {
	wp.maxConnNum =maxConnNum
	wp.maxIdletime = maxIdletime
	wp.functor = functor
	wp.endpoints = make(map[string]EndPointInfo)
}

func (wp *WConnPool) AddHost(host string, maxFreqLimit uint32, fuseRate float64) {
	ep := EndPointInfo{}
	ep.Init(host, wp.maxConnNum, maxFreqLimit, defaultMinFreqLimit, fuseRate, defaultMaxIdleTime, defaultAliveCheckInterval, wp.functor)
	wp.m.Lock()
	defer wp.m.Unlock()
	wp.endpoints[host] = ep
}

func (wp *WConnPool) GetConn(key string, waitTime time.Duration) (string, ConnWarp, error) {
	wp.cursor = (wp.cursor + 1) % uint32(len(wp.endpoints))
	i := uint32(0)
	first := true
	for host, endpoint := range wp.endpoints {
		i++
		if i == wp.cursor && !first {
			first = false
			conn, err := endpoint.GetConn(key, waitTime)
			if conn != NilConn && err == nil {
				return host, conn, NilConnError
			}
		}
	}
	return "", NilConn, NilConnError
}
