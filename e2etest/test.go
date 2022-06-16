package main

import (
	"./wconnpool"
	"context"
	"fmt"
	"time"

	//"git.woa.com/warrenyan/wconnpool"

	"github.com/go-redis/redis"
	//	"time"
)

func CreateRedisConn(host string) (wconnpool.Conn, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     host,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return rdb, nil
}

//func AliveCheck(client *redis.Client) bool {
func AliveCheck(conn wconnpool.Conn) bool {
	client, ok := conn.(*redis.Client)
	if(!ok){
		panic("invalid conn")
	}
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	return err == nil
}

func CloseRedisConn(conn wconnpool.Conn) error {
	client, ok := conn.(*redis.Client)
	if(!ok){
		panic("invalid conn")
	}
	return client.Close()
}

func main() {
	pool := new(wconnpool.WConnPool)
	pool.Init(5, CreateRedisConn, CloseRedisConn, AliveCheck)
	pool.AddHost("localhost:9010", 5, 0.5)
	pool.AddHost("localhost:9012", 5, 0.5)
	cnt := 0
	for {
		cnt += 1
		conn, err := pool.GetConn(time.Second)
		if err != nil {
			fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Get conn error:", err)
			time.Sleep(time.Second)
			continue
		}
		client, ok := conn.GetRaw().(*redis.Client)
		if !ok {
			panic("invalid client")
		}
		ctx := context.Background()
		if _, err := client.Set(ctx, "key", cnt, 0).Result(); err != nil {
			fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Error: ", conn.GetHost(), err)
			conn.IncrFail()
		} else{
			conn.IncrSuc()
			fmt.Println(time.Now().Format("2006-01-02 15:04:05"),"[get and set suc] cnt:", cnt, time.Now(), conn.GetHost())
		}

		conn.Putback()
	}

	/*
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:9012",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ctx := context.Background()
	err := rdb.Set(ctx, "score", 100, 0).Err()
	if err != nil {
		fmt.Printf("set score failed, err:%v\n", err)
	}
	val, err := rdb.Get(ctx, "score").Result()
	fmt.Println("Get Result:", val, err)
	*/

}