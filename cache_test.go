/**
* @file lrucache.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version
* @date 2016-08-09
 */

package mini_spider

import (
	"fmt"
	"gopkg.in/redis.v4"
	"testing"
)

func TestCache(t *testing.T) {
	addr := "hz01-spider-coverage4.hz01:6379"
	opts := &redis.Options{
		Addr:     addr,
		DB:       3,
		Password: "lishuo",
	}
	redis := NewRedisDB(opts)
	cache := NewCache(redis, &DefaultActions{}, LinkCacheType)
	if i, err := cache.Get("http://aleda.cn/"); err == nil {
		fmt.Println(i, err)
	}
	return
}

func TestRedisDB(t *testing.T) {
	addr := "hz01-spider-coverage4.hz01:6379"
	opts := &redis.Options{
		Addr:     addr,
		DB:       3,
		Password: "lishuo",
	}
	redis := redis.NewClient(opts)
	if pong, err := redis.Ping().Result(); err == nil {
		fmt.Println(pong, err)
	}
	if r, err := redis.Set("-1", "cache-1", 0).Result(); err != nil {
		fmt.Println(r, err)
	}
	if r, err := redis.Set("0", "cache0", 0).Result(); err != nil {
		fmt.Println(r, err)
	}
	if r, err := redis.Get("0").Result(); err != nil {
		fmt.Println(r, err)
	} else {
		fmt.Println(r)
	}

	if r, err := redis.Get("2").Result(); err != nil {
		fmt.Println(r, err)
	} else {
		fmt.Println(r)
	}

	redisDB := NewRedisDB(opts)
	if b, err := redisDB.Get("0"); err != nil {
		fmt.Println(b, err)
	} else {
		fmt.Println(b)
	}
	keys, err := redisDB.Scan()
	if err == nil {
		fmt.Println(len(keys))
	}
}
