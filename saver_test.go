/**
* @file worker.go
* @brief every host dose correspond to a worker.
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version
* @date 2016-08-09
 */

package mini_spider

import (
	"gopkg.in/redis.v4"
	"sync"
	"testing"
	"time"
)

func TestSaver(t *testing.T) {
	addr := "10.210.84.15:6379"
	linkdb, pagedb := 3, 4
	passwd := "lishuo"

	redisLinkOpts := &redis.Options{
		Addr:     addr,
		DB:       linkdb,
		Password: passwd,
	}
	redisLinkDB := NewRedisDB(redisLinkOpts)
	redisPageOpts := &redis.Options{
		Addr:     addr,
		DB:       pagedb,
		Password: passwd,
	}
	redisPageDB := NewRedisDB(redisPageOpts)

	// 后续考虑有没有可替代的db，可以既存储link，又存储page

	link := NewLink("http://aleda.cn/")
	s := NewSaverWithDB(redisLinkDB, redisPageDB, nil)
	s.stop = make(chan struct{})
	s.Links = newLinkChannel()
	s.Pages = make(chan *Page, 5)
	s.wg = &sync.WaitGroup{}
	s.Links.stack(link)

	page := NewPage("http://aleda.cn/")
	s.Pages <- page

	s.wg.Add(1)
	go s.Run()
	go s.Links.stack(link)
	go s.Links.stack(link)
	s.Links.stack(link)
	s.Links.stack(link)
	time.Sleep(10 * time.Second)
	if s.IsRunning {
		s.stop <- struct{}{}
	}
	return
}
