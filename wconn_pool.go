package wconnpool

import (
	"fmt"
	"sync"
	"time"
)

var (
	NilConn      = ConnWarp{}
	NilConnError = fmt.Errorf("Nil Connection")
	LimitError   = fmt.Errorf("reach limit")
)

/*
type Conn interface {
	Close() error
	AliveCheck() bool
}
*/

type Conn interface{}
type CreateConnFunctor func(string) (Conn, error)
type CloseConnFunctor func(conn Conn) error
type AliveCheckFunctor func(conn Conn) bool
type FuseAlert func(host string, key string, msg string)

type ConnWarp struct {
	conn     interface{}
	deadLine time.Time
	endPoint *EndPointInfo
}

func IsNil(conn ConnWarp) bool {
	return conn == NilConn
}

func (cw *ConnWarp) Close() (err error) {
	cw.endPoint.closeConn(*cw)
	cw.conn = nil
	return
}

func (cw *ConnWarp) GetRaw() Conn {
	return cw.conn
}

// 检查连接是否过期, true:该链接已过期
func (cw *ConnWarp) CheckTtl() bool {
	return cw.deadLine.Before(time.Now())
}

func (cw *ConnWarp) CheckAlive() bool {
	if cw.endPoint != nil && cw.endPoint.checkFunctor != nil {
		return cw.endPoint.checkFunctor(cw.conn)
	}
	return true
}

func (cw *ConnWarp) IncrSuc() {
	cw.endPoint.IncrSuc()
}

func (cw *ConnWarp) IncrSucWithKey(key string) {
	cw.endPoint.IncrSucWithKey(key)
}

func (cw *ConnWarp) IncrFail() {
	cw.endPoint.IncrFail()
}

func (cw *ConnWarp) IncrFailWithKey(key string) {
	cw.endPoint.IncrFailWithKey(key)
}

func (cw *ConnWarp) GetHost() string {
	if cw.endPoint != nil {
		return cw.endPoint.name
	}
	return ""
}

func (cw *ConnWarp) Putback() {
	if cw.endPoint == nil {
		return
	}
	cw.endPoint.PutConn(*cw)
}

type StateInfo struct {
	ep           *EndPointInfo
	key          string
	curTime      int64
	sucCount     uint32
	failCount    uint32
	curLimit     uint32
	maxFreqLimit uint32
	minFreqLimit uint32
	fuseRate     float64
	fuseAlert    FuseAlert
}

func (si *StateInfo) Init(ep *EndPointInfo, key string, maxFreqLimitPerSecond uint32, minFreqLimitPerSecond uint32, fuseRate float64) {
	si.key = key
	si.ep = ep
	si.maxFreqLimit = maxFreqLimitPerSecond
	si.curTime = time.Now().Unix()
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
	if (si.sucCount+si.failCount) >= 2 && float64(si.sucCount*1.0)/float64(si.sucCount+si.failCount) > si.fuseRate {
		if si.curLimit*2 <= si.maxFreqLimit {
			si.curLimit = si.curLimit * 2
			var host string
			if si.ep != nil {
				host = si.ep.name
			}
			fmt.Printf("[RECOVER] %s current limit: %d\n", host, si.curLimit)
		} else {
			si.curLimit = si.maxFreqLimit
		}
	}
}

func (si *StateInfo) Info() {
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
	if (si.sucCount+si.failCount) > 2 && (float64(si.sucCount)*1.0)/float64(si.sucCount+si.failCount) < si.fuseRate {
		var host string
		if si.ep != nil {
			host = si.ep.name
		}

		if si.curLimit/2.0 >= si.minFreqLimit {
			si.curLimit = si.curLimit / 2.0
		} else {
			si.curLimit = si.minFreqLimit
		}
		if si.fuseAlert != nil {
			si.fuseAlert(host, si.key, fmt.Sprintf("current limit: %d", si.curLimit))
		} else {
			fmt.Printf("[FUSE] %s current limit: %d\n", host, si.curLimit)
		}
	}
}

// 是否达到频次限制，true表示达到了频次限制
func (si *StateInfo) reachLimit() bool {
	if time.Now().Unix() != si.curTime {
		si.curTime = time.Now().Unix()
		si.sucCount = 0
		si.failCount = 0
	}
	return si.sucCount+si.failCount >= si.curLimit
}

type EndPointInfo struct {
	name          string
	maxConnNum    uint32
	curConnNum    uint32
	oneStat       StateInfo
	states        map[string]StateInfo
	maxFreqLimit  uint32
	minFreqLimit  uint32
	fuseRate      float64
	conns         chan ConnWarp
	createFunctor CreateConnFunctor
	closeFunctor  CloseConnFunctor
	checkFunctor  AliveCheckFunctor
	maxIdleTime   time.Duration
	checkInterval time.Duration
	m             *sync.Mutex
}

func (ep *EndPointInfo) Init(host string, maxConnNum uint32, maxFreqLimit uint32, minFreqLimit uint32, fuseRate float64, maxIdleTime time.Duration, aliveCheckInterval time.Duration,
	createFunctor CreateConnFunctor, closeFunctor CloseConnFunctor, checkFunctor AliveCheckFunctor) {
	ep.name = host
	ep.maxConnNum = maxConnNum
	ep.curConnNum = 0
	ep.maxFreqLimit = maxFreqLimit
	ep.minFreqLimit = minFreqLimit
	ep.fuseRate = fuseRate
	ep.maxIdleTime = maxIdleTime
	ep.conns = make(chan ConnWarp, maxConnNum)
	ep.checkInterval = aliveCheckInterval
	ep.createFunctor = createFunctor
	ep.closeFunctor = closeFunctor
	ep.checkFunctor = checkFunctor
	ep.states = make(map[string]StateInfo)
	ep.oneStat.Init(ep, "", maxFreqLimit, minFreqLimit, fuseRate)
	ep.m = new(sync.Mutex)

	if checkFunctor != nil {
		go func() {
			ep.checkAliveConn()
		}()
	}
}

func (ep *EndPointInfo) createConn() (ConnWarp, error) {
	conn, err := ep.createFunctor(ep.name)
	if err != nil {
		fmt.Printf("%s create conn err: %s\n", ep.name, err )
		return NilConn, err
	}
	ep.curConnNum += 1
	//fmt.Printf("[create conn] %s ep:%v, curConnNum: %d, max_limit: %d, conn num:%d\n", ep.name, ep, ep.curConnNum, ep.maxConnNum, len(ep.conns))
		cw := ConnWarp{conn: conn, deadLine: time.Now().Add(ep.maxIdleTime), endPoint: ep}
	return cw, nil
}

func (ep *EndPointInfo) closeConn(conn ConnWarp) error {
	if ep.closeFunctor != nil {
		err := ep.closeFunctor(conn.GetRaw())
		if err != nil {
			fmt.Println("CLOSE ERROR:", err,"cur_conn_num: ",ep.curConnNum, "conn num:", len(ep.conns))
		}
		if ep.curConnNum > 0 {
			ep.curConnNum -= 1
		}
		//fmt.Println("CLOSE ################: ", ep.name, "cur_conn_num: ", ep.curConnNum, "conn num:", len(ep.conns))
		return err
	}
	return nil
}

func (ep *EndPointInfo) Close() {
	close(ep.conns)
}

func (ep *EndPointInfo) updateConnTTl(conn ConnWarp) {
	conn.deadLine = time.Now().Add(ep.maxIdleTime)
}

func (ep *EndPointInfo) getConn(waitTime time.Duration) (ConnWarp, error) {
	//fmt.Printf("[getConn] %s ep:%v, curConnNum: %d, max_limit: %d, conn num:%d\n", ep.name, unsafe.Pointer(ep), ep.curConnNum, ep.maxConnNum, len(ep.conns))
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
	case <-time.After(waitTime):
		return NilConn, NilConnError
	}
}

func (ep *EndPointInfo) GetConn(waitTime time.Duration) (ConnWarp, error) {
	for {
		if ep.reachLimit() {
			return NilConn, LimitError
		}
		//fmt.Println("no reachLimit: ep:", (ep.name), unsafe.Pointer(ep), unsafe.Pointer(&(ep.oneStat)), ep.oneStat.curTime, ep.name, ep.oneStat.sucCount, ep.oneStat.failCount, ep.oneStat.curLimit)
		conn, err := ep.getConn(waitTime)
		if err != nil || conn == NilConn {
			return NilConn, NilConnError
		}
		//当前连接过期，关闭该连接，从池子里重新拿一个连接
		if conn.CheckTtl() {
			conn.Close()
			continue
		}
		return conn, nil
	}
}

func (ep *EndPointInfo) GetConnWithKey(key string, waitTime time.Duration) (ConnWarp, error) {
	for {
		if ep.reachLimitWithKey(key) {
			return NilConn, NilConnError
		}
		conn, err := ep.getConn(waitTime)
		if err != nil {
			return NilConn, NilConnError
		}
		if conn != NilConn && err != nil && conn.CheckTtl() {
			continue
		}
		return conn, nil
	}
}

func (ep *EndPointInfo) putConn(conn ConnWarp) error {
	if !conn.CheckTtl() {
		ep.conns <- conn
		//fmt.Printf("[putConn] %s ep:%v  curConnNum:%d, conn num: %d\n", ep.name, unsafe.Pointer(ep), ep.curConnNum, len(ep.conns))
		return nil
	}
	return conn.Close()
}

func (ep *EndPointInfo) PutConn(conn ConnWarp) error {
	return ep.putConn(conn)
}

func (ep *EndPointInfo) checkAliveConn() {
	for {
		<-time.After(ep.checkInterval)
		select {
		case conn := <-ep.conns:
			if conn.CheckTtl() && conn.CheckAlive() {
				ep.conns <- conn
			} else {
				ep.closeConn(conn)
			}
		default:
			break
		}
	}
}

func (ep *EndPointInfo) reachLimit() bool {
	return ep.oneStat.reachLimit()
}

func (ep *EndPointInfo) reachLimitWithKey(key string) bool {
	state, ok := ep.states[key]
	if ok {
		return state.reachLimit()
	}
	state = StateInfo{}
	state.Init(ep, key, ep.maxFreqLimit, ep.minFreqLimit, ep.fuseRate)
	ep.states[key] = state
	return state.reachLimit()
}

func (ep *EndPointInfo) IncrSuc() {
	ep.m.Lock()
	defer ep.m.Unlock()
	ep.oneStat.IncrSuc()
}

func (ep *EndPointInfo) IncrSucWithKey(key string) {
	ep.m.Lock()
	defer ep.m.Unlock()
	state, ok := ep.states[key]
	if !ok {
		state = StateInfo{}
		state.Init(ep, key, ep.maxFreqLimit, ep.minFreqLimit, ep.fuseRate)
	}
	state.IncrSuc()
	ep.states[key] = state
}

func (ep *EndPointInfo) IncrFail() {
	ep.m.Lock()
	defer ep.m.Unlock()
	ep.oneStat.IncrFail()
}

func (ep *EndPointInfo) IncrFailWithKey(key string) {
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
}

type WConnPool struct {
	maxIdletime   time.Duration
	maxConnNum    uint32
	createFunctor CreateConnFunctor
	closeFunctor  CloseConnFunctor
	checkFunctor  AliveCheckFunctor
	endpoints     []*EndPointInfo
	m             sync.Mutex
	cursor        uint32
}

const (
	defaultMaxIdleTime        time.Duration = time.Minute
	defaultAliveCheckInterval time.Duration = time.Second
	defaultMinFreqLimit       uint32        = 1
)

func (wp *WConnPool) Init(maxConnNum uint32, createFunctor CreateConnFunctor, closeFunctor CloseConnFunctor, checkFunctor AliveCheckFunctor) {

	wp.maxConnNum = maxConnNum
	wp.createFunctor = createFunctor
	wp.closeFunctor = closeFunctor
	wp.checkFunctor = checkFunctor
	wp.endpoints = make([]*EndPointInfo, 0, 0)
}

func (wp *WConnPool) AddHost(host string, maxFreqLimit uint32, fuseRate float64) {
	ep := new(EndPointInfo)
	ep.Init(host, wp.maxConnNum, maxFreqLimit, defaultMinFreqLimit, fuseRate, defaultMaxIdleTime, defaultAliveCheckInterval,
		wp.createFunctor, wp.closeFunctor, wp.checkFunctor)
	wp.m.Lock()
	defer wp.m.Unlock()
	wp.endpoints = append(wp.endpoints, ep)
}

func (wp *WConnPool) PutConn(cw ConnWarp) {
	cw.Putback()
}

func (wp *WConnPool) GetConn(waitTime time.Duration) (ConnWarp, error) {
	for i := 0; i < len(wp.endpoints); i += 1 {
		endpoint := wp.endpoints[wp.cursor]
		conn, err := endpoint.GetConn(waitTime)
		wp.cursor = (wp.cursor + 1) % uint32(len(wp.endpoints))
		if conn != NilConn && err == nil {
			return conn, err
		}
	}
	return NilConn, NilConnError
}

func (wp *WConnPool) GetConnWithKey(key string, waitTime time.Duration) (ConnWarp, error) {
	wp.cursor = (wp.cursor + 1) % uint32(len(wp.endpoints))
	i := uint32(0)
	first := true
	for _, endpoint := range wp.endpoints {
		i++
		if i == wp.cursor && !first {
			first = false
			conn, err := endpoint.GetConnWithKey(key, waitTime)
			if conn != NilConn && err == nil {
				return conn, NilConnError
			}
		}
	}
	return NilConn, NilConnError
}
