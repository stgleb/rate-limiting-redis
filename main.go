package main

import (
	redis "gopkg.in/redis.v5"
	//"os"
	//"io/ioutil"
	//"log"
	"time"
	"fmt"
)

func overLimit(client *redis.Client, resourceName string, duration int, limit int64) bool {
	key := fmt.Sprintf("%s:%d:%d", resourceName, duration, time.Now().Unix())
	count := client.Incr(key)
	client.Expire(key, time.Duration(duration) * time.Second)

	if count.Val() > limit {
		return false
	}

	return true
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	//f, err := os.OpenFile("limits.lua", os.O_RDWR, 0666)
	//
	//if err != nil {
	//	log.Printf("Error opening lua script for limits %s", err.Error())
	//}
	//data, err := ioutil.ReadAll(f)
	//
	//if err != nil {
	//	log.Printf("Error reading lua script for limits %s", err.Error())
	//}
	//
	//defer f.Close()
	//limits := []interface{}{0, 0, 0, 0, 1, time.Now().Unix()}
	//keys := []string{"resource"}

	//scriptCmd := client.ScriptLoad(string(data))
	//cmd := client.Eval(string(data), keys, limits...)
	//log.Print(cmd.String())
	//result, err := cmd.Result()
	//log.Print(cmd.Val())
	//log.Print(result, err)

	for i := 0;i < 10; i++ {
		fmt.Println(overLimit(client, "resource", 1, 1))
		time.Sleep(time.Duration(1) * time.Second)
	}
}
