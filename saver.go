/**
* @file saver.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version 1.0.0
* @date 2016-08-10
 */

package mini_spider

import (
	"sync"
	"sync/atomic"
	"time"
)

// Default Saver options
const (
	DefaultSaverIdleTTL   time.Duration = 0 * time.Second
	DefaultLinkExpiration time.Duration = 0 * time.Second
	DefaultPageExpiration time.Duration = 0 * time.Second
)

// saver result
// Communication from saver to the master crawler
type SaverResponse struct {
	data      interface{} // link or page
	idleDeath bool        // death
	saverID   int         // saver id
}

// The saver itself, used to save links and web pages.
type Saver struct {
	Index          int           // index
	Links          linkChannel   // links job channel
	Pages          chan *Page    // pages job channel
	LinkCache      *Cache        // Link cache
	PageCache      *Cache        // Page cache
	IsRunning      bool          // is running
	OnBuzy         int32         // is buzy
	LinkExpiration time.Duration // Link expiration
	PageExpiration time.Duration // Page expiration

	resp          chan *SaverResponse                    // saver result
	stop          chan struct{}                          // stop signal
	workerIdleTTL time.Duration                          // ilde timeout
	logFunc       func(LogFlags, string, ...interface{}) // logger
	wg            *sync.WaitGroup                        // wg
	acts          Actions                                // actions
}

func NewSaverWithCache(linkCache *Cache, pageCache *Cache, acts Actions) *Saver {
	if acts == nil {
		acts = NewDefaultActions()
	}
	return &Saver{
		Index:         0,
		LinkCache:     linkCache,
		PageCache:     pageCache,
		workerIdleTTL: DefaultSaverIdleTTL,
		logFunc:       getDefaultLogFunc(LogInfo),
		acts:          acts,
	}
}

func NewSaverWithDB(linkDB KVStorage, pageDB KVStorage, acts Actions) *Saver {
	if acts == nil {
		acts = NewDefaultActions()
	}
	return &Saver{
		Index:         0,
		LinkCache:     NewCache(linkDB, acts, LinkCacheType),
		PageCache:     NewCache(pageDB, acts, PageCacheType),
		workerIdleTTL: DefaultSaverIdleTTL,
		logFunc:       getDefaultLogFunc(LogInfo),
		acts:          acts,
	}
}

type linkChannel chan []*Link

func newLinkChannel() linkChannel {
	return make(chan []*Link, 1)
}

func (l linkChannel) stack(links ...*Link) {
	toStack := links
	for {
		select {
		case l <- toStack:
			return
		case old := <-l:
			toStack = append(old, toStack...)
		}
	}
}

func (s *Saver) init() {
	s.IsRunning = true
}

func (s *Saver) mergeLink(link *Link) {
	//url := link.URL.String()
	url := link.URL
	newLink := link
	if oldLink, err := s.LinkCache.Get(url); err == nil {
		s.logFunc(LogInfo, "url:%s has old link.", url)
		tempLink := oldLink.(Link)
		newLink = s.acts.MergeLink(link, &tempLink)
	} else {
		s.logFunc(LogInfo, "get url: %s info failed: %s", url, err)
	}
	// TODO 返回SaverResponse
	if err := s.LinkCache.Set(url, newLink, s.LinkExpiration); err == nil { // set 成功
		s.logFunc(LogInfo, "Set link suuccess: %s", url)
	} else { // set 失败
		s.logFunc(LogInfo, "Set link %s failed: %s", url, err)
	}
	return
}

func (s *Saver) mergePage(page *Page) {
	//url := link.URL.String()
	url := page.URL
	newPage := page
	if oldPage, err := s.PageCache.Get(url); err == nil {
		s.logFunc(LogInfo, "url:%s has old page.", url)
		tempPage := oldPage.(Page)
		newPage = s.acts.MergePage(page, &tempPage)
	} else {
		s.logFunc(LogInfo, "get page: %s info failed: %s", url, err)
	}
	// TODO 返回SaverResponse
	if err := s.PageCache.Set(url, newPage, s.PageExpiration); err == nil { // set 成功
		s.logFunc(LogInfo, "Set page suuccess: %s", url)
	} else { // set 失败
		s.logFunc(LogInfo, "Set page %s failed: %s", url, err)
	}
	return
}

func (s *Saver) Run() {
	defer func() {
		s.logFunc(LogInfo, "Saver Done.")
		s.IsRunning = false
		s.wg.Done()
	}()
	s.init()

	for {
		var idleChan <-chan time.Time
		s.logFunc(LogInfo, "Waiting for saving jobs.")

		if s.workerIdleTTL > 0 {
			s.logFunc(LogInfo, "Idlettl set: %d", s.workerIdleTTL)
			idleChan = time.After(s.workerIdleTTL)
		}

		select {
		case <-s.stop:
			s.IsRunning = false
			s.logFunc(LogInfo, "Saver receive the stop signal.")
			return
		case <-idleChan:
			s.IsRunning = false
			s.logFunc(LogInfo, "Saver running timeout.")
			s.sendResponse(nil, true, s.Index)
			return
		case links := <-s.Links:
			// TODO 可以对这一批links进行去重
			atomic.AddInt32(&s.OnBuzy, 1)
			for i, link := range links {
				s.logFunc(LogInfo, "Merge the %d link: %s", i, link.URL)
				s.mergeLink(link)
				select {
				case <-s.stop:
					s.logFunc(LogInfo, "stop signal received.")
					s.IsRunning = false
					return
				default:
					// Nothing
				}
			}
			atomic.AddInt32(&s.OnBuzy, -1)
		case page := <-s.Pages:
			atomic.AddInt32(&s.OnBuzy, 2)
			s.mergePage(page)
			atomic.AddInt32(&s.OnBuzy, -2)
		}
	}
}

// Send a response to the crawler.
func (s *Saver) sendResponse(i interface{}, idleDeath bool, id int) {
	select {
	case <-s.stop:
		s.logFunc(LogInfo, "ignoring send response, saver will stop.")
		return
	default:
		// Nothing, just continue...
	}
	// No stop signal, send the response
	res := &SaverResponse{
		i,
		idleDeath,
		id,
	}
	s.logFunc(LogInfo, "send saver res")
	s.resp <- res
}
