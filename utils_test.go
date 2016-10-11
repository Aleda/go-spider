/**
* @file worker.go
* @brief every host dose correspond to a worker.
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version
* @date 2016-08-09
 */

package mini_spider

import (
	"github.com/PuerkitoBio/purell"
	"testing"
)

func TestToURLContexts(t *testing.T) {
	urls := []string{"http://www.baidu.com/",
		"http://www.sina.com.cn/"}
	urlContexts := toURLContexts(urls, nil, purell.FlagsAllGreedy)
	assertEqual(t, len(urlContexts), 2)
	assertEqual(t, urlContexts[0].URL().String(), "http://www.baidu.com/")
	assertEqual(t, urlContexts[1].URL().String(), "http://www.sina.com.cn/")
	assertEqual(t, urlContexts[0].NormalizedURL().String(), "http://baidu.com")
	assertEqual(t, urlContexts[1].NormalizedURL().String(), "http://sina.com.cn")
	return
}

func TestGetMainDomainFromHost(t *testing.T) {
	assertEqual(t, GetMainDomainFromHost("www.baidu.com"), "baidu.com")
	assertEqual(t, GetMainDomainFromHost("www.nc.jx.cn"), "nc.jx.cn")
	assertEqual(t, GetMainDomainFromHost("www.jx.cn"), "www.jx.cn")
	assertEqual(t, GetMainDomainFromHost("sf.gg"), "sf.gg")
	assertEqual(t, GetMainDomainFromHost("image.google.com.hk"), "google.com.hk")
}
