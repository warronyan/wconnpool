package wconnpool

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestStateInfo(t *testing.T) {
	key := "test"
	st := &StateInfo{}

	st.Init(nil, key, 10, 2, 0.5)
	fmt.Printf("%v\n", st)


	for i := 0; i < 10 ; i++ {
		if st.reachLimit() == true {
			t.Errorf("error reachLimit: false")
		}
		st.IncrSuc()
		st.Info()
	}

	if st.reachLimit() == false {
		t.Errorf("errro reachLimit: true")
	}

	fmt.Printf("%v\n", st)

	for i := 0; i < 12 ; i++ {
		st.IncrFail()
		st.Info()
	}

	if st.reachLimit() == false {
		t.Errorf("errro reachLimit: true")
	}
	time.Sleep(time.Second)
	fmt.Println("========================")
	if st.reachLimit() == true {
		t.Errorf("errro reachLimit: false")
	}
	st.IncrSuc()
	st.Info()
	if st.reachLimit() == true {
		t.Errorf("errro reachLimit: false")
	}
	st.IncrSuc()
	st.Info()
}

func createTestConn(string) (Conn, error) {
	conn := &TestErrConn{
		a: rand.Intn(100),
	}
	return conn, nil
}

type TestErrConn struct {
	a int
}

func (conn *TestErrConn) Close() error {
	return nil
}

func (conn *TestErrConn) AliveCheck() bool {
	return true
}

func TestEndPointInfo(t *testing.T) {
	ep := &EndPointInfo{}
	ep.Init("host1", 10, 100, 1, 0.5, time.Minute, time.Minute, createTestConn)
	for i := 0; i< 10; i += 1 {
		c, err := ep.GetConn(time.Second)
		if err != nil {
			t.Error("Invalid GetConn")
			break
		}
		c.IncrSuc()
		//fmt.Println("===================", c.endPoint)
	}
	// 超过最大连接数时，无法创新新的连接
	_, err := ep.GetConn(time.Second)
	if err == nil {
		t.Error("Invalid GetConn")
	}
	ep1 := &EndPointInfo{}
	ep1.Init("host1", 10, 100, 1, 0.5, time.Minute, time.Minute, createTestConn)
	c, err := ep1.GetConn(time.Second)
	if err != nil {
		t.Error("Invalid GetConn")
	}
	for i := 0; i < 100; i+=1 {
		c.IncrSuc()
	}
	// 超过最大请求次数
	c, err = ep1.GetConn( 0)
	if err == nil {
		t.Error("Invalid GetConn")
	}
	time.Sleep(time.Second)
	// 过了一分钟了，又恢复配额了
	c, err = ep1.GetConn( 0)
	if err != nil {
		t.Error("Invalid GetConn")
	}

	//  失败后熔断
	for i := 0; i < 10; i+=1 {
		c.IncrFail()
	}
	c, err = ep1.GetConn(0)
	if err == nil {
		t.Error("Invalid GetConn")
	}
	c, err = ep1.GetConn(0)
	if err == nil {
		t.Error("Invalid GetConn")
	}

	// 间隔一秒后, 重新计算熔断，但是限速不变
	time.Sleep(time.Second)
	c, err = ep1.GetConn( 0)
	if err != nil {
		t.Error("Invalid GetConn")
	}
	c.IncrFail()
	c, err = ep1.GetConn( 0)
	if err == nil {
		t.Error("Invalid GetConn")
	}

	ep2 := &EndPointInfo{}
	ep2.Init("host1", 10, 100, 1, 0.5, time.Minute, time.Minute, createTestConn)
	c, err = ep2.GetConn(0)
	ep2.PutConn(c)

	//放到池子里的连接，下一次能能取出来
	c1, _ := ep2.GetConn(0)
	if c != c1 {
		t.Error("Invalid GetConn")
	}
}

func TestWConnPool(t *testing.T) {
	pool := WConnPool{}
	pool.Init(100, time.Minute, createTestConn)
	pool.AddHost("host1", 10, 0.5)
	pool.AddHost("host2", 10, 0.5)
	cw1, _  := pool.GetConn(0)
	if cw1.GetHost() != "host1" {
		t.Error("Invalid Host")
	}

	cw1.Putback()

	cw2, _  := pool.GetConn(0)
	if cw2.GetHost() != "host2" {
		t.Error("Invalid Host")
	}
	cw2.Putback()

	fmt.Println(cw1.endPoint.name)

	for i:=0; i<10;i++ {
		cw1.IncrFail()
		cw1.IncrFail()
	}

	cw3, _  := pool.GetConn(0)
	fmt.Println(cw3.GetHost())
	if cw3.GetHost() != "host2" {
		t.Error("Invalid Host")
	}
	defer cw3.Putback()

	// 1秒钟后，host1至少会保留1秒1次的配额，因此会重新给host1一次机会
	time.Sleep(time.Second)
	cw4, _ := pool.GetConn(0)
	if cw4.GetHost() != "host1" {
		t.Error("Invalid Host")
	}
	// host1这次再失败
	cw4.IncrFail()
	// 后面不会再给host1机会了；
	for i:=0; i<10;i +=1 {
		cw, _ := pool.GetConn(0)
		cw.Putback()
		if cw.GetHost() == "host1" {
			t.Error("Invalid Host")
		}
	}

	time.Sleep(time.Second)
	cw5, _ := pool.GetConn(0)
	cw5.Putback()
	cw5.IncrSuc()
	cw5.IncrSuc()
	cw5.IncrSuc()
	// host1频次控制增加到 4次
	cw , _ := pool.GetConn(0)
	if cw.GetHost() != "host2" {
		t.Error("Invalid Host")
	}
	cw , _ = pool.GetConn(0)
	if cw.GetHost() != "host1" {
		t.Error("Invalid Host")
	}
	cw , _ = pool.GetConn(0)
	if cw.GetHost() != "host2" {
		t.Error("Invalid Host")
	}
	cw , _ = pool.GetConn(0)
	if cw.GetHost() != "host1" {
		t.Error("Invalid Host")
	}
	cw , _ = pool.GetConn(0)
	if cw.GetHost() != "host2" {
		t.Error("Invalid Host")
	}

	cw5.IncrFail()
	// host1配额已满，不会再取host1
	cw , _ = pool.GetConn(0)
	if cw.GetHost() != "host2" {
		t.Error("Invalid Host")
	}
}
