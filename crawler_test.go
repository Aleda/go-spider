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
	"net/url"
	"testing"
	//	"time"
)

func TestCrawler(t *testing.T) {
	urls := []string{"http://movie.douban.com/chart",
		"http://movie.douban.com/review/best/"}

	acts := new(DefaultActions)
	opts := NewOptions()
	c := NewCrawler(opts, acts)
	c.Run(urls)
	fmt.Println(c)
	return
}

func TestCrawler1(t *testing.T) {
	urls := []string{"http://pycm.baidu.com:8081"}

	acts := new(DefaultActions)
	opts := NewOptions()
	c := NewCrawler(opts, acts)
	c.Run(urls)
}

func TestCrawler2(t *testing.T) {
	urls := []string{"http://pycm.baidu.com:8081"}

	acts := new(DefaultActions)
	opts := NewOptions()
	c := NewCrawler(opts, acts)

	//ctx, _ := stringToURLContext("http://pycm.baidu.com:8081", nil, DefaultNormalizationFlags)
	//res := &WorkerResponse{
	//ctx,
	//false,
	//nil,
	//"",
	//false,
	//}
	//ctxs := []*URLContext{ctx}
	//c.logFunc = getLogFunc(c.Actions, c.Options.LogFlags, -1)
	//c.init(ctxs)
	//c.save(res)
	//c.save(res)
	//c.save(res)
	//time.Sleep(5 * time.Second)

	c.Run(urls)
}

func TestIsSameHost(t *testing.T) {
	url1, _ := url.Parse("https://a.movie.douban.com/subject/26628357/")
	urlSource1, _ := url.Parse("https://b.movie.douban.com/chart")
	fmt.Println(url1.Host)
	fmt.Println(urlSource1.Host)
	fmt.Println(url1)
}
