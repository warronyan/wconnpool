package wconnpool

import (
	"fmt"
	"testing"
	"time"
	"git.code.oa.com/trpc-go/trpc-go"
)

func TestStateInfo(t *testing.T) {
	key := "test"
	st := &StateInfo{}

	st.Init(nil, key, 10, 2, 0.5)
	fmt.Printf("%v\n", st)


	for i := 0; i < 10 ; i++ {
		if st.reachLimit() == false {
			t.Errorf("error reachLimit: false")
		}
		st.IncrSuc()
		st.Info()
	}

	if st.reachLimit() {
		t.Errorf("errro reachLimit: true")
	}

	fmt.Printf("%v\n", st)

	for i := 0; i < 12 ; i++ {
		st.IncrFail()
		st.Info()
	}

	if st.reachLimit() {
		t.Errorf("errro reachLimit: true")
	}
	time.Sleep(time.Second)
	fmt.Println("========================")
	if !st.reachLimit() {
		t.Errorf("errro reachLimit: false")
	}
	st.IncrSuc()
	st.Info()
	if !st.reachLimit() {
		t.Errorf("errro reachLimit: false")
	}
	st.IncrSuc()
	st.Info()
}

func createTestConn(string) (Conn, error) {

}

type TestConn struct {}

func (conn *TestConn) Close() error {
	return nil
}

func (conn *TestConn) AliveCheck() bool {
	return true
}

func TestEndPointInfo(t *testing.T) {
	ep = &EndPointInfo{}
	ep.Init("host1", 10, 100, 1, 0.5, time.Minute, time.Minute, TestEndPointInfo)
}
