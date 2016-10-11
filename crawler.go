/**
* @file crawler.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version 1.0.0
* @date 2016-08-10
 */

package mini_spider

import (
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"gopkg.in/redis.v4"
)

// The crawler itself, the master of the whole process
type Crawler struct {
	Options   *Options
	Actions   Actions
	LinkCache *Cache //link cache
	PageCache *Cache //page cache

	// Internal fields
	logFunc         func(LogFlags, string, ...interface{})
	push            chan *WorkerResponse
	saverResponse   chan *SaverResponse // Saver result
	enqueue         chan interface{}
	stop            chan struct{}
	wg              *sync.WaitGroup
	wgSaver         *sync.WaitGroup
	pushPopRefCount int
	visits          int // total number of visiting

	// keep lookups in maps, O(1) access time vs O(n) for slice. The empty struct value
	// is of no use, but this is the smallest type possible - it uses no memory at all.
	visited map[string]struct{}
	hosts   map[string]struct{}
	workers map[string]*Worker
	savers  map[int]*Saver // Savers
}

// Crawler constructor with a pre-initialized Options object.
func NewCrawler(opts *Options, acts Actions) *Crawler {
	ret := new(Crawler)
	ret.Options = opts
	ret.Actions = acts
	return ret
}

// Run starts the crawling process, based on the given seeds and the current
// Options settings. Execution stops either when MaxVisits is reached (if specified)
// or when no more URLs need visiting. If an error occurs, it is returned (if
// MaxVisits is reached, the error ErrMaxVisits is returned).
func (c *Crawler) Run(seeds interface{}) error {
	// Helper log function, takes care of filtering based on level
	c.logFunc = getLogFunc(c.Actions, c.Options.LogFlags, 1, "crawler")

	seeds = c.Actions.Start(seeds)
	ctxs := toURLContexts(seeds, nil, c.Options.URLNormalizationFlags)
	c.init(ctxs)

	// Start with the seeds, and loop till death
	c.enqueueUrls(ctxs)
	err := c.collectUrls()

	c.Actions.End(err)
	return err
}

// Initialize the Crawler's internal fields before a crawling execution.
func (c *Crawler) init(ctxs []*URLContext) {
	// Initialize the internal hosts map
	c.hosts = make(map[string]struct{}, len(ctxs))
	for _, ctx := range ctxs {
		// Add c normalized URL's host if it is not already there.
		if _, ok := c.hosts[ctx.normalizedURL.Host]; !ok {
			c.hosts[ctx.normalizedURL.Host] = struct{}{}
		}
	}

	hostCount := len(c.hosts)
	l := len(ctxs)
	c.logFunc(LogTrace, "init() - seeds length: %d", l)
	c.logFunc(LogTrace, "init() - host count: %d", hostCount)
	c.logFunc(LogInfo, "robot user-agent: %s", c.Options.RobotUserAgent)

	// Create a shiny new WaitGroup
	c.wg = new(sync.WaitGroup)
	c.wgSaver = new(sync.WaitGroup)

	// Initialize the visits fields
	c.visited = make(map[string]struct{}, l)
	c.pushPopRefCount, c.visits = 0, 0

	// Create the workers map and the push channel (the channel used by workers
	// to communicate back to the crawler)
	c.stop = make(chan struct{})
	if c.Options.SameHostOnly {
		c.workers, c.push = make(map[string]*Worker, hostCount),
			make(chan *WorkerResponse, hostCount)
		c.savers, c.saverResponse = make(map[int]*Saver, hostCount),
			make(chan *SaverResponse, hostCount)
	} else {
		c.workers, c.push = make(map[string]*Worker, c.Options.HostBufferFactor*hostCount),
			make(chan *WorkerResponse, c.Options.HostBufferFactor*hostCount)
		c.savers, c.saverResponse = make(map[int]*Saver, c.Options.HostBufferFactor*hostCount),
			make(chan *SaverResponse, c.Options.HostBufferFactor*hostCount)
	}

	redisLinkOpts := &redis.Options{
		Addr:     c.Options.RedisDBAddr,
		DB:       c.Options.LinkID,
		Password: c.Options.RedisPasswd,
	}
	redisLinkDB := NewRedisDB(redisLinkOpts)
	redisPageOpts := &redis.Options{
		Addr:     c.Options.RedisDBAddr,
		DB:       c.Options.PageID,
		Password: c.Options.RedisPasswd,
	}
	redisPageDB := NewRedisDB(redisPageOpts)

	c.LinkCache = NewCache(redisLinkDB, c.Actions, LinkCacheType)
	c.PageCache = NewCache(redisPageDB, c.Actions, PageCacheType)
	// Create and pass the enqueue channel c.enqueue = make(chan interface{}, c.Options.EnqueueChanBuffer)
	c.setActionsEnqueueChan()
}

// Set the Enqueue channel on the extender, based on the naming convention.
func (c *Crawler) setActionsEnqueueChan() {
	defer func() {
		if err := recover(); err != nil {
			// Panic can happen if the field exists on a pointer struct, but that
			// pointer is nil.
			c.logFunc(LogError, "cannot set the enqueue channel: %s", err)
		}
	}()

	// Using reflection, check if the extender has a `EnqueueChan` field
	// of type `chan<- interface{}`. If it does, set it to the crawler's
	// enqueue channel.
	v := reflect.ValueOf(c.Actions)
	el := v.Elem()
	if el.Kind() != reflect.Struct {
		c.logFunc(LogInfo, "Actions is not a struct, cannot set the enqueue channel")
		return
	}
	ec := el.FieldByName("EnqueueChan")
	if !ec.IsValid() {
		c.logFunc(LogInfo, "Actions.EnqueueChan does not exist, cannot set the enqueue channel")
		return
	}
	t := ec.Type()
	if t.Kind() != reflect.Chan || t.ChanDir() != reflect.SendDir {
		c.logFunc(LogInfo, "Actions.EnqueueChan is not of type chan<-interface{}, cannot set the enqueue channel")
		return
	}
	tt := t.Elem()
	if tt.Kind() != reflect.Interface || tt.NumMethod() != 0 {
		c.logFunc(LogInfo, "extender.EnqueueChan is not of type chan<-interface{}, cannot set the enqueue channel")
		return
	}
	src := reflect.ValueOf(c.enqueue)
	ec.Set(src)
}

// Launch a new worker goroutine for a given host.
func (c *Crawler) launchWorker(ctx *URLContext) *Worker {
	// Initialize index and channels
	i := len(c.workers) + 1
	jobs := newJobChannel()

	// Create the worker
	w := &Worker{
		host:    ctx.normalizedURL.Host,
		id:      i,
		push:    c.push,
		jobs:    jobs,
		stop:    c.stop,
		enqueue: c.enqueue,
		wg:      c.wg,
		logFunc: getLogFunc(c.Actions, c.Options.LogFlags, i, "worker"),
		opts:    c.Options,
		acts:    c.Actions,
	}

	// Increment wait group count
	c.wg.Add(1)

	// Launch worker
	go w.run()
	c.logFunc(LogInfo, "worker %d launched for host %s", i, w.host)
	c.workers[w.host] = w

	return w
}

//
// Check if the specified URL is from the same host as its source URL, or if
// nil, from the same host as one of the seed URLs.
func (c *Crawler) isSameHost(ctx *URLContext) bool {
	// If there is a source URL, then just check if the new URL is from the same host
	if ctx.normalizedSourceURL != nil {
		return GetMainDomainFromHost(ctx.normalizedURL.Host) == GetMainDomainFromHost(ctx.normalizedSourceURL.Host)
	}

	// Otherwise, check if the URL is from one of the seed hosts
	_, ok := c.hosts[ctx.normalizedURL.Host]
	return ok
}

// Enqueue the URLs returned from the worker, as long as it complies with the
// selection policies.
func (c *Crawler) enqueueUrls(ctxs []*URLContext) (cnt int) {
	for _, ctx := range ctxs {
		var isVisited, enqueue bool

		// Cannot directly enqueue a robots.txt URL, since it is managed as a special case
		// in the worker (doesn't return a response to crawler).
		if ctx.IsRobotsURL() {
			continue
		}
		// Check if it has been visited before, using the normalized URL
		// 全局去重
		_, isVisited = c.visited[ctx.normalizedURL.String()]

		// Filter the URL
		if enqueue = c.Actions.Filter(ctx, isVisited); !enqueue {
			// Filter said NOT to use c url, so continue with next
			c.logFunc(LogIgnored, "ignore on filter policy(default visited): %s", ctx.normalizedURL)
			continue
		}

		// Even if filter said to use the URL, it still MUST be absolute, http(s)-prefixed,
		// and comply with the same host policy if requested.
		// TODO 支持将https => http
		// TODO 相对路径的问题
		if !ctx.normalizedURL.IsAbs() {
			// Only absolute URLs are processed, so ignore
			c.logFunc(LogIgnored, "ignore on absolute policy: %s", ctx.normalizedURL)
			// TODO https exprotocol
		} else if !strings.HasPrefix(ctx.normalizedURL.Scheme, "http") {
			c.logFunc(LogIgnored, "ignore on scheme policy: %s", ctx.normalizedURL)

		} else if c.Options.SameHostOnly && !c.isSameHost(ctx) {
			// Only allow URLs coming from the same host
			c.logFunc(LogIgnored, "ignore on same host policy: %s", ctx.normalizedURL)

		} else {
			// All is good, visit c URL (robots.txt verification is done by worker)

			// Possible caveat: if the normalization changes the host, it is possible
			// that the robots.txt fetched for c host would differ from the one for
			// the unnormalized host. However, c should be rare, and is a weird
			// behaviour from the host (i.e. why would site.com differ in its rules
			// from www.site.com) and can be fixed by using a different normalization
			// flag. So c is an acceptable behaviour for gocrawl.

			// Launch worker if required, based on the host of the normalized URL
			w, ok := c.workers[ctx.normalizedURL.Host]
			if !ok {
				// No worker exists for c host, launch a new one
				w = c.launchWorker(ctx)
				// Automatically enqueue the robots.txt URL as first in line
				if robCtx, e := ctx.getRobotsURLCtx(); e != nil {
					c.Actions.Error(newCrawlError(ctx, e, CekParseRobots))
					c.logFunc(LogError, "ERROR parsing robots.txt from %s: %s", ctx.normalizedURL, e)
				} else {
					c.logFunc(LogInfo, "enqueue: %s", robCtx.url)
					c.Actions.Enqueued(robCtx)
					w.jobs.stack(robCtx)
				}
			}

			cnt++
			c.logFunc(LogInfo, "enqueue: %s", ctx.url)
			c.Actions.Enqueued(ctx)
			w.jobs.stack(ctx)
			c.pushPopRefCount++

			// Once it is stacked, it WILL be visited eventually, so add it to the visited slice
			// (unless denied by robots.txt, but c is out of our hands, for all we
			// care, it is visited).
			if !isVisited {
				// The visited map works with the normalized URL
				c.visited[ctx.normalizedURL.String()] = struct{}{}
			}
		}
	}
	return
}

// This is the main loop of the crawler, waiting for responses from the workers
// and processing these responses.
func (c *Crawler) collectUrls() error {
	defer func() {
		c.logFunc(LogInfo, "waiting for goroutines to complete...")
		c.wg.Wait()
		c.wgSaver.Wait()
		c.logFunc(LogInfo, "crawler done.")
	}()

	for {
		// By checking c after each channel reception, there is a bug if the worker
		// wants to reenqueue following an error or a redirection. The pushPopRefCount
		// temporarily gets to zero before the new URL is enqueued. Check the length
		// of the enqueue channel to see if c is really over, or just c temporary
		// state.
		//
		// Check if refcount is zero - MUST be before the select statement, so that if
		// no valid seeds are enqueued, the crawler stops.
		if c.pushPopRefCount == 0 && len(c.enqueue) == 0 {
			c.logFunc(LogInfo, "sending STOP signals...")
			close(c.stop)
			return nil
		}

		select {
		case res := <-c.push:
			// Received a response, check if it contains URLs to enqueue
			if res.visited {
				c.visits++
				if c.Options.MaxVisits > 0 && c.visits >= c.Options.MaxVisits {
					// Limit reached, request workers to stop
					c.logFunc(LogInfo, "visits: %d, sending STOP signals...", c.visits)
					close(c.stop)
					return ErrMaxVisits
				}
			}
			if res.idleDeath {
				// The worker timed out from its Idle TTL delay, remove from active workers
				delete(c.workers, res.host)
				c.logFunc(LogInfo, "worker for host %s cleared on idle policy", res.host)
			} else {
				// crawl at once
				c.enqueueUrls(toURLContexts(res.harvestedURLs, res.ctx.url, c.Options.URLNormalizationFlags))
				c.pushPopRefCount--
				c.save(res)
			}

		case enq := <-c.enqueue:
			// Received a command to enqueue a URL, proceed
			ctxs := toURLContexts(enq, nil, c.Options.URLNormalizationFlags)
			c.logFunc(LogTrace, "receive url(s) to enqueue %v", ctxs)
			c.enqueueUrls(ctxs)
		case saverResp := <-c.saverResponse:
			c.logFunc(LogInfo, "resp receive from saver")
			// saver response, TODO
			if saverResp.idleDeath {
				delete(c.savers, saverResp.saverID)
			}
		case <-c.stop:
			c.logFunc(LogInfo, "receive STOP signals...")
			return ErrInterrupted
		}
	}
	panic("unreachable")
}

func (c *Crawler) Stop() {
	defer func() {
		if err := recover(); err != nil {
			c.logFunc(LogError, "error when manually stopping crawler: %s", err)
		}
	}()

	// c channel may be closed already
	close(c.stop)
}

func (c *Crawler) launchSaver() *Saver {
	i := len(c.savers) + 1
	linkChannel := newLinkChannel()
	page := make(chan *Page, 3)
	saver := &Saver{
		Index:          i,
		Links:          linkChannel,
		Pages:          page,
		LinkCache:      c.LinkCache,
		PageCache:      c.PageCache,
		IsRunning:      false,
		OnBuzy:         0,
		LinkExpiration: c.Options.LinkExpiration,
		PageExpiration: c.Options.PageExpiration,
		resp:           c.saverResponse,
		stop:           c.stop,
		workerIdleTTL:  c.Options.SaverIdleTTL,
		logFunc:        getLogFunc(c.Actions, c.Options.LogFlags, i, "saver"),
		wg:             c.wgSaver,
		acts:           c.Actions,
	}
	c.wgSaver.Add(1)
	go saver.Run()
	c.logFunc(LogInfo, "Saver %d launched.", i)
	c.savers[i] = saver
	return saver
}

func (c *Crawler) save(res *WorkerResponse) {
	var followLinks []string
	mapString := func(raw interface{}) {
		switch v := raw.(type) {
		case *url.URL:
			followLinks = []string{v.String()}
		case []*url.URL:
			r := make([]string, 0, len(v))
			for _, u := range v {
				r = append(r, u.String())
			}
			followLinks = r
		}
	}
	mapString(res.harvestedURLs)
	URL := ""
	if res.ctx.NormalizedURL() != nil {
		URL = res.ctx.NormalizedURL().String()
	}
	sourceURL := ""
	if res.ctx.NormalizedSourceURL() != nil {
		sourceURL = res.ctx.NormalizedSourceURL().String()
	}
	state := res.ctx.State
	crawlTime := res.ctx.Crawltime

	// status
	statusLink := &Link{
		URL:       URL,
		SourceURL: sourceURL,
		IsNew:     false,
		State:     state,
		Intime:    time.Now(),
		Crawltime: crawlTime,
	}
	// filter follow links, TODO

	// page
	page := &Page{
		URL:        URL,
		Links:      followLinks,
		Intime:     time.Now(),
		Updatetime: time.Now(),
		Page:       string(res.ctx.page),
	}

	isLinkSended, isPageSened := false, false
	for _, s := range c.savers {
		if !s.IsRunning {
			continue
		}
		// send to save
		if s.OnBuzy&1 != 1 {
			s.Links.stack(statusLink)
			isLinkSended = true
		}
		// send to save
		if s.OnBuzy&2 != 1 {
			s.Pages <- page
			isPageSened = true
		}
	}
	if !isLinkSended || !isPageSened {
		s := c.launchSaver()
		if !isLinkSended {
			s.Links.stack(statusLink)
		}
		if !isPageSened {
			s.Pages <- page
		}
	}
}
