package main

import (
	"fmt"
	redis "gopkg.in/redis.v5"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

var client *redis.Client
var script string

type Limit struct {
	Duration int64
	Count    int64
}

func init() {
	f, err := os.OpenFile("limits.lua", os.O_RDWR, 0666)

	if err != nil {
		log.Printf("Error opening lua script for limits %s", err.Error())
	}
	data, err := ioutil.ReadAll(f)

	if err != nil {
		log.Printf("Error reading lua script for limits %s", err.Error())
	}

	defer f.Close()

	if len(data) > 0 {
		script = string(data)
	}
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
		if overLimit(client, resourceName, limit.Duration, limit.Count) {
			return true
		}
	}

	return false
}

func Example1() {
	limits := []Limit{{Duration: 1, Count: 1}, {Duration: 60, Count: 5}}
	counter := 0

	for counter < 10 {
		if overLimitMulti(client, "resource", limits) == false {
			fmt.Println("Ok")
			counter++
		}

		time.Sleep(time.Duration(1) * time.Second)
	}
}

// Check limits by script, evaluate script all the time
func overLimitScript() bool {
	limits := []interface{}{1, 5, 0, 0, 1, time.Now().Unix()}
	keys := []string{"resource"}

	cmd := client.Eval(script, keys, limits...)

	if cmd.Val() == nil {
		return false
	}

	return true
}

// Optimized version, loads script once
func loadScript(script string) func(*redis.Client, []string, []int) bool {
	sha := ""

	return func(client *redis.Client, resource []string, limits []int) bool {
		tmp := []interface{}{0, 0, 0, 0, 1, time.Now().Unix()}

		for i, limit := range limits {
			tmp[i] = limit
		}

		if len(sha) == 0 {
			sha = client.ScriptLoad(script).Val()
		}
		cmd := client.EvalSha(sha, resource, tmp...)

		_, err := cmd.Result()

		// If script not found execute
		if err != nil && strings.Contains(err.Error(), "NOSCRIPT") {
			cmd = client.Eval(script, resource, tmp...)
		}

		if cmd.Val() == nil {
			return false
		}

		return true
	}
}

func main() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	counter := 10
	overLimitFn := loadScript(script)

	for counter > 0 {
		if overLimitFn(client, []string{"resource"}, []int{1, 5, 0, 0}) == false {
			log.Print("Ok")
			counter--
		}
	}
}
