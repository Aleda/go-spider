/**
* @file worker.go
* @brief every worker dose correspond to a host.
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version
* @date 2016-08-09
 */

package mini_spider

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/temoto/robotstxt.go"
	"golang.org/x/net/html"
	"path"
	"strings"
)

// Communication from worker to the master crawler, about the crawling of a URL
type WorkerResponse struct {
	ctx           *URLContext
	visited       bool
	harvestedURLs interface{}
	host          string
	idleDeath     bool
}

// Delay information: the Options delay, the Robots.txt delay, and the last delay used.
type DelayInfo struct {
	OptsDelay   time.Duration
	RobotsDelay time.Duration
	LastDelay   time.Duration
}

// Fetch information: the duration of the fetch, the returned status code, whether or
// not it was a HEAD request, and whether or not it was a robots.txt request.
type FetchInfo struct {
	Ctx           *URLContext
	Duration      time.Duration
	StatusCode    int
	IsHeadRequest bool
}

// The worker is dedicated to fetching and visiting a given host, respecting
// this host's robots.txt crawling policies.
type Worker struct {
	// Worker identification
	host string // host
	id   int    // id of workers

	// Communication channels and sync
	push    chan<- *WorkerResponse
	jobs    jobChannel
	stop    chan struct{}
	enqueue chan<- interface{}
	wg      *sync.WaitGroup

	// Robots validation
	robotsGroup *robotstxt.Group

	// Logging
	logFunc func(LogFlags, string, ...interface{})

	// Implementation fields
	wait           <-chan time.Time
	lastFetch      *FetchInfo
	lastCrawlDelay time.Duration
	opts           *Options
	acts           Actions // Strategy Implementation
}

// the constructor of worker
func NewWorker(host string, opts *Options, acts Actions) *Worker {
	if acts == nil {
		acts = NewDefaultActions()
	}
	return &Worker{
		host, 0, nil, nil, nil, nil, nil, nil, nil, nil, nil, 0, opts, acts,
	}
}

// TODO stack or queue?
// The pop channel is a stacked channel used by workers to pop the next URL(s)
// to process.
type jobChannel chan []*URLContext

// Constructor to create and initialize a jobChannel
func newJobChannel() jobChannel {
	// The pop channel is stacked, so only a buffer of 1 is required
	// see http://gowithconfidence.tumblr.com/post/31426832143/stacked-channels
	return make(chan []*URLContext, 1)
}

// The stack function ensures the specified URLs are added to the job channel
// with minimal blocking (since the channel is stacked, it is virtually equivalent
// to an infinitely buffered channel).
func (c jobChannel) stack(cmd ...*URLContext) {
	toStack := cmd
	for {
		select {
		case c <- toStack:
			return
		case old := <-c:
			// Content of the channel got emptied and is now in old, so append whatever
			// is in toStack to it, so that it can either be inserted in the channel,
			// or appended to some other content that got through in the meantime.
			toStack = append(old, toStack...)
		}
	}
}

// Start crawling the host.
func (w *Worker) run() {
	defer func() {
		w.logFunc(LogInfo, "Worker done.")
		w.wg.Done()
	}()

	w.init()
	for {
		var idleChan <-chan time.Time

		w.logFunc(LogInfo, "Waiting for crawling jobs...")

		// worker TTL
		if w.opts.WorkerIdleTTL > 0 {
			idleChan = time.After(w.opts.WorkerIdleTTL)
		}

		select {
		case <-w.stop: // stop
			w.logFunc(LogInfo, "Stop signal received.")
			return
		case <-idleChan:
			w.logFunc(LogInfo, "Idle timeout received.")
			w.sendResponse(nil, false, nil, true)
			return
		case batch := <-w.jobs: // distribute jobs
			w.logFunc(LogInfo, "Get %d jobs.", len(batch))
			for i, ctx := range batch {
				w.logFunc(LogInfo, "Get %d Job: url: %s", i, ctx.url)
				// TODO
				// 判断是否robots封禁，以及对命中robots封禁的应对策略

				/*
					if ctx.IsRobotsURL() {
						w.requestRobotsTxt(ctx)
					} else if this.isAllowedPerRobotsPolicies(ctx.url) {
						w.requestUrl(ctx)
					} else {
						// Must still notify Crawler that this URL was processed, although not visited
						w.acts.Disallowed(ctx)
						w.sendResponse(ctx, false, nil, false)
					}
				*/
				w.requestUrl(ctx)
				select {
				case <-w.stop:
					w.logFunc(LogInfo, "stop signal received.")
					return
				default:
					// Nothing, just continue...
				}

			}
		}
	}
}

// Initialize the worker's internal fields
func (w *Worker) init() {
	if w.push == nil {
		w.push = make(chan<- *WorkerResponse)
	}
	if w.jobs == nil {
		w.jobs = newJobChannel()
	}
	if w.stop == nil {
		w.stop = make(chan struct{})
	}
	if w.enqueue == nil {
		w.enqueue = make(chan<- interface{})
	}
	if w.wg == nil {
		w.wg = &sync.WaitGroup{}
	}
	if w.robotsGroup == nil {
		w.robotsGroup = &robotstxt.Group{}
	}
	if w.opts == nil {
		w.opts = NewOptions()
	}
	if w.acts == nil {
		w.acts = NewDefaultActions()
	}
	if w.logFunc == nil {
		w.logFunc = getLogFunc(w.acts, DefaultLogLevel, w.id, "worker")
	}
}

// Set the crawl delay between this request and the next.
func (w *Worker) setCrawlDelay() {
	var robDelay time.Duration

	if w.robotsGroup != nil {
		robDelay = w.robotsGroup.CrawlDelay
	}
	w.lastCrawlDelay = w.acts.ComputeDelay(w.host,
		&DelayInfo{
			w.opts.CrawlDelay,
			robDelay,
			w.lastCrawlDelay,
		},
		w.lastFetch)
	w.logFunc(LogInfo, "using crawl-delay: %v", w.lastCrawlDelay)
}

// Request the specified URL and return the http.Response.
func (w *Worker) fetchUrl(ctx *URLContext) (res *http.Response, ok bool) {
	var e error
	var silent bool

	for {
		// 抓取压力设置，根据抓取状态策略方来算出下一次抓取的间隔
		w.logFunc(LogTrace, "Waiting for crawl delay.")
		if w.wait != nil {
			<-w.wait
			w.wait = nil
		}

		w.setCrawlDelay()

		now := time.Now()

		ctx.Crawltime = now
		if res, e = w.acts.Fetch(ctx); e != nil {
			w.logFunc(LogInfo, "fetch fail")
			// Check if this is an ErrEnqueueRedirect, in which case we will enqueue
			// the redirect-to URL.
			//TODO 应该要有专门的跳转策略
			if ue, y := e.(*url.Error); y {
				// We have a *url.Error, check if it was returned because of an ErrEnqueueRedirect
				if ue.Err == ErrEnqueueRedirect {
					// Do not notify this error outside of this if block, this is not a
					// "real" error. We either enqueue the new URL, or fail to parse it,
					// and then stop processing the current URL.
					silent = true
					// Parse the URL in the context of the original URL (so that relative URLs are ok).
					// Absolute URLs that point to another host are ok too.
					if ur, e := ctx.url.Parse(ue.URL); e != nil {
						// Notify error
						w.acts.Error(newCrawlError(nil, e, CekParseRedirectURL))
						w.logFunc(LogError, "ERROR parsing redirect URL %s: %s", ue.URL, e)
					} else {
						// Enqueue the redirect-to URL
						w.logFunc(LogTrace, "redirect to %s", ur)
						w.enqueue <- ur
					}
				}
			}

			// No fetch, so set to nil
			w.lastFetch = nil

			if !silent {
				// Notify error
				w.acts.Error(newCrawlError(ctx, e, CekFetch))
				w.logFunc(LogError, "ERROR fetching %s: %s", ctx.url, e)
			}

			// Return from this URL crawl
			//w.sendResponse(ctx, false, nil, false)
			return nil, false

		} else {
			// Get the fetch duration
			w.logFunc(LogInfo, "fetch succ")
			fetchDuration := time.Now().Sub(now)
			// Crawl delay starts now.
			w.wait = time.After(w.lastCrawlDelay)

			// Keep trace of this last fetch info
			w.lastFetch = &FetchInfo{
				ctx,
				fetchDuration,
				res.StatusCode,
				ctx.HeadBeforeGet,
			}
			ok = true
		}
		if ctx.HeadBeforeGet {
			// Close the HEAD request's body
			defer res.Body.Close()
			// Next up is GET request, maybe
			ctx.HeadBeforeGet = false
			// Ask caller if we should proceed with a GET
			if !w.acts.RequestGet(ctx, res) {
				w.logFunc(LogIgnored, "ignored on HEAD filter policy: %s", ctx.url)
				w.sendResponse(ctx, false, nil, false)
				ok = false
				break
			}
		} else {
			ok = true
			break
		}
	}
	return
}

// Process the response for a URL.
// TODO 提供一个base版本，然后交给策略方去提供就好了。
func (w *Worker) visitUrl(ctx *URLContext, res *http.Response) interface{} {
	var doc *goquery.Document
	var harvested interface{}
	var doLinks bool

	// Load a goquery document and call the visitor function
	if bd, e := ioutil.ReadAll(res.Body); e != nil {
		w.acts.Error(newCrawlError(ctx, e, CekReadBody))
		w.logFunc(LogError, "ERROR reading body %s: %s", ctx.url, e)
	} else {
		// cp page content
		ctx.page = bd
		if node, e := html.Parse(bytes.NewBuffer(bd)); e != nil {
			w.acts.Error(newCrawlError(ctx, e, CekParseBody))
			w.logFunc(LogError, "ERROR parsing %s: %s", ctx.url, e)
		} else {
			doc = goquery.NewDocumentFromNode(node)
			doc.Url = res.Request.URL
		}
		// Re-assign the body so it can be consumed by the visitor function
		res.Body = ioutil.NopCloser(bytes.NewBuffer(bd))
	}

	// Visit the document (with nil goquery doc if failed to load)
	if harvested, doLinks = w.acts.Visit(ctx, res, doc); doLinks {
		// Links were not processed by the visitor, so process links
		if doc != nil {
			harvested = w.processLinks(doc)
		} else {
			w.acts.Error(newCrawlErrorMessage(ctx, "No goquery document to process links.", CekProcessLinks))
			w.logFunc(LogError, "ERROR processing links %s", ctx.url)
		}
	}
	fmt.Println(harvested)
	// Notify that w URL has been visited
	w.acts.Visited(ctx, harvested)

	return harvested
}

// Process the specified URL.
// 失败重试，跳转等操作在这里进行
func (w *Worker) requestUrl(ctx *URLContext) {
	if res, ok := w.fetchUrl(ctx); ok { // 抓取成功
		w.logFunc(LogInfo, "Fetch success.")
		var harvested interface{}
		var visited bool

		// Close the body on function end
		defer res.Body.Close()

		// Any 2xx status code is good to go
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			// Success, visit the URL
			harvested = w.visitUrl(ctx, res)
			visited = true
		} else {
			// Error based on status code received
			w.acts.Error(newCrawlErrorMessage(ctx, res.Status, CekHttpStatusCode))
			w.logFunc(LogError, "ERROR status code for %s: %s", ctx.url, res.Status)
		}
		w.sendResponse(ctx, visited, harvested, false)
	} else { // 失败重试
		w.logFunc(LogInfo, "Fetch failed.")
		w.sendResponse(ctx, false, nil, false)
	}
}

func handleBaseTag(rootURL string, baseHref string, aHref string) string {
	root, _ := url.Parse(rootURL)
	resolvedBase, _ := root.Parse(baseHref)

	parsedURL, _ := url.Parse(aHref)
	// If a[href] starts with a /, it overrides the base[href]
	if parsedURL.Host == "" && !strings.HasPrefix(aHref, "/") {
		aHref = path.Join(resolvedBase.Path, aHref)
	}

	resolvedURL, _ := resolvedBase.Parse(aHref)
	return resolvedURL.String()
}

// Scrape the document's content to gather all links
func (w *Worker) processLinks(doc *goquery.Document) (result []*url.URL) {
	baseUrl, _ := doc.Find("base[href]").Attr("href")
	urls := doc.Find("a[href]").Map(func(_ int, s *goquery.Selection) string {
		val, _ := s.Attr("href")
		if baseUrl != "" {
			val = handleBaseTag(doc.Url.String(), baseUrl, val)
		}
		return val
	})
	for _, s := range urls {
		// If href starts with "#", then it points to w same exact URL, ignore (will fail to parse anyway)
		if len(s) > 0 && !strings.HasPrefix(s, "#") {
			if parsed, e := url.Parse(s); e == nil {
				parsed = doc.Url.ResolveReference(parsed)
				result = append(result, parsed)
			} else {
				w.logFunc(LogIgnored, "ignore on unparsable policy %s: %s", s, e.Error())
			}
		}
	}
	return
}

// Send a response to the crawler.
func (w *Worker) sendResponse(ctx *URLContext, visited bool, harvested interface{}, idleDeath bool) {
	// Push harvested urls back to crawler, even if empty (uses the channel communication
	// to decrement reference count of pending URLs)
	// TODO Robots?
	if ctx == nil || !isRobotsURL(ctx.url) {
		// If a stop signal has been received, ignore the response, since the push
		// channel may be full and could block indefinitely.
		select {
		case <-w.stop:
			w.logFunc(LogInfo, "ignoring send response, will stop.")
			return
		default:
			// Nothing, just continue...
		}

		// No stop signal, send the response
		res := &WorkerResponse{
			ctx,
			visited,
			harvested,
			w.host,
			idleDeath,
		}
		w.logFunc(LogInfo, "send worker res")
		w.push <- res
	}
}
