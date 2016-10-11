/**
* @file worker.go
* @brief every host dose correspond to a worker.
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version
* @date 2016-08-09
 */

package mini_spider

import (
	"fmt"
	"github.com/PuerkitoBio/purell"
	"sync"
	"testing"
	"time"
)

func TestWorker(t *testing.T) {
	urls := []string{"http://pycm.baidu.com:8081"}
	urlContexts := toURLContexts(urls, nil, purell.FlagsAllGreedy)
	jobs := newJobChannel()

	host := "movie.douban.com"
	opts := NewOptions()
	s := make(chan struct{})
	wg := new(sync.WaitGroup)
	push := make(chan *WorkerResponse)

	w := NewWorker(host, opts, &DefaultActions{})
	w.wg = wg
	w.stop = s
	w.jobs = jobs
	w.logFunc = getDefaultLogFunc(LogInfo)
	w.push = push
	exit := make(chan int)
	go func() {
		for {
			select {
			case resp := <-push:
				fmt.Println("hehe")
				fmt.Println(resp)
			case <-exit:
				return
			}
		}
	}()
	wg.Add(1)
	go w.run()
	jobs.stack()
	jobs.stack(urlContexts...)
	time.Sleep(1 * time.Second)
	s <- struct{}{}
	exit <- 1
	wg.Wait()
	return
}

func TestWorker1(t *testing.T) {
	urls := []string{"http://pycm.baidu.com:8081", "http://pycm.baidu.com:8081/page1.html"}
	ctxs := NewURLContext(urls)
	opts := NewOptions()
	acts := NewDefaultActions()
	exit := make(chan int)
	resp := make(chan *WorkerResponse)

	w := NewWorker("pycm.baidu.com", opts, acts)
	w.push = resp
	go w.run()
	go func() {
		for {
			select {
			case resp := <-resp:
				fmt.Println(resp)
			case <-exit:
				return
			}
		}
	}()
	time.Sleep(1 * time.Second)
	go w.jobs.stack(ctxs...)
	go w.jobs.stack(ctxs...)
	time.Sleep(25 * time.Second)
	exit <- 1
}
