package main

import (
	redis "gopkg.in/redis.v5"
	//"os"
	//"io/ioutil"
	//"log"
	"fmt"
	"time"
)

type Limit struct {
	duration int64
	count    int64
}

func overLimit(client *redis.Client, resourceName string, duration int64, limit int64) bool {
	key := fmt.Sprintf("%s:%d:%d", resourceName, duration, time.Now().Unix()/duration)
	count := client.Incr(key)
	client.Expire(key, time.Duration(duration)*time.Second)

	if count.Val() > limit {
		return true
	}

	return false
}

func overLimitMulti(client *redis.Client, resourceName string, limits []Limit) bool {
	for _, limit := range limits {
		if overLimit(client, resourceName, limit.duration, limit.count) {
			return true
		}
	}

	return false
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
	limits := []Limit{{duration: 1, count: 1}, {duration: 60, count: 5}}

	counter := 0

	for counter < 10 {
		if overLimitMulti(client, "resource", limits) == false {
			fmt.Println("Ok")
			counter++
		}

		time.Sleep(time.Duration(1) * time.Second)
	}
}
