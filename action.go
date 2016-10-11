/**
* @file action.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version 1.0.0
* @date 2016-08-10
 */

package mini_spider

import (
	"bytes"
	"encoding/gob"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// The default HTTP client used by DefaultActions's fetch requests (this is thread-safe).
// The client's fields can be customized (i.e. for a different redirection strategy, a
// different Transport object, ...). It should be done prior to starting the crawler.
var HttpClient = &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
	// For robots.txt URLs, allow up to 10 redirects, like the default http client.
	// Rationale: the site owner explicitly tells us that this specific robots.txt
	// should be used for this domain.
	if isRobotsURL(req.URL) {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if len(via) > 0 {
			req.Header.Set("User-Agent", via[0].Header.Get("User-Agent"))
		}
		return nil
	}

	// For all other URLs, do NOT follow redirections, the default Fetch() implementation
	// will ask the worker to enqueue the new (redirect-to) URL. Returning an error
	// will make httpClient.Do() return a url.Error, with the URL field containing the new URL.
	return ErrEnqueueRedirect
}}

// Default working implementation of an extender.
type DefaultActions struct {
	EnqueueChan chan<- interface{}
	//	Logger      *log.Logger
}

// Return the same seeds as those received (those that were passed
// to Run() initially).
func (da *DefaultActions) Start(seeds interface{}) interface{} {
	return seeds
}

// End is a no-op.
func (da *DefaultActions) End(err error) {}

// Error is a no-op (logging is done automatically, regardless of the implementation
// of the Error() hook).
func (da *DefaultActions) Error(err *CrawlError) {}

// Log prints to the standard error by default, based on the requested log verbosity.
func (da *DefaultActions) Log(logFlags LogFlags, msgLevel LogFlags, msg string) {
	var prefix string
	switch msgLevel {
	case LogError:
		prefix = "[Error]"
	case LogIgnored:
		prefix = "[FILTR]"
	case LogInfo:
		prefix = "[INFO] "
	case LogTrace:
		prefix = "[TRACE]"
	}
	if logFlags&msgLevel == msgLevel {
		log.Println(prefix, msg)
	}
}

// ComputeDelay returns the delay specified in the Crawler's Options, unless a
// crawl-delay is specified in the robots.txt file, which has precedence.
func (da *DefaultActions) ComputeDelay(host string, di *DelayInfo, lastFetch *FetchInfo) time.Duration {
	if di.RobotsDelay > 0 {
		return di.RobotsDelay
	}
	return di.OptsDelay
}

// Fetch requests the specified URL using the given user agent string. It uses
// a custom http Client instance that doesn't follow redirections. Instead, the
// redirected-to URL is enqueued so that it goes through the same Filter() and
// Fetch() process as any other URL.
//
// Two options were considered for the default Fetch() implementation :
// 1- Not following any redirections, and enqueuing the redirect-to URL,
//    failing the current call with the 3xx status code.
// 2- Following all redirections, enqueuing only the last one (where redirection
//    stops). Returning the response of the next-to-last request.
//
// Ultimately, 1) was implemented, as it is the most generic solution that makes
// sense as default for the library. It involves no "magic" and gives full control
// as to what can happen, with the disadvantage of having the Filter() being aware
// of all possible intermediary URLs before reaching the final destination of
// a redirection (i.e. if A redirects to B that redirects to C, Filter has to
// allow A, B, and C to be Fetched, while solution 2 would only have required
// Filter to allow A and C).
//
// Solution 2) also has the disadvantage of fetching twice the final URL (once
// while processing the original URL, so that it knows that there is no more
// redirection HTTP code, and another time when the actual destination URL is
// fetched to be visited).
func (da *DefaultActions) Fetch(ctx *URLContext) (*http.Response, error) {
	var reqType string

	// Prepare the request with the right user agent
	if ctx.HeadBeforeGet {
		reqType = "HEAD"
	} else {
		reqType = "GET"
	}
	req, e := http.NewRequest(reqType, ctx.url.String(), nil)
	if e != nil {
		return nil, e
	}
	for k, v := range ctx.Headers {
		req.Header.Set(k, v)
	}
	return HttpClient.Do(req)
}

// Ask the worker to actually request the URL's body (issue a GET), unless
// the status code is not 2xx.
func (da *DefaultActions) RequestGet(ctx *URLContext, headRes *http.Response) bool {
	return headRes.StatusCode >= 200 && headRes.StatusCode < 300
}

// Ask the worker to actually request (fetch) the Robots.txt document.
func (da *DefaultActions) RequestRobots(ctx *URLContext, robotAgent string) (data []byte, doRequest bool) {
	return nil, true
}

// FetchedRobots is a no-op.
func (da *DefaultActions) FetchedRobots(ctx *URLContext, res *http.Response) {}

// Enqueue the URL if it hasn't been visited yet.
func (da *DefaultActions) Filter(ctx *URLContext, isVisited bool) bool {
	return !isVisited
}

// Enqueued is a no-op.
func (da *DefaultActions) Enqueued(ctx *URLContext) {}

// Ask the worker to harvest the links in this page.
func (da *DefaultActions) Visit(ctx *URLContext, res *http.Response, doc *goquery.Document) (harvested interface{}, findLinks bool) {
	return nil, true
}

// Visited is a no-op.
func (da *DefaultActions) Visited(ctx *URLContext, harvested interface{}) {}

// Disallowed is a no-op.
func (da *DefaultActions) Disallowed(ctx *URLContext) {}

func (da *DefaultActions) Marshal(i interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	var err error
	if err = encoder.Encode(i); err == nil {
		return buf.Bytes(), nil
	}
	return nil, err
}

func (da *DefaultActions) Unmarshal(b []byte, i interface{}) error {
	decoder := gob.NewDecoder(bytes.NewBuffer(b))
	var err error
	if err = decoder.Decode(i); err == nil {
		return nil
	}
	return err
}

// Merge two Link
func (da *DefaultActions) MergeLink(newLink *Link, oldLink *Link) *Link {
	return newLink
}

// Merge two page
func (da *DefaultActions) MergePage(newPage *Page, oldPage *Page) *Page {
	return newPage
}
