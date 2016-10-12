/**
* @file worker.go
* @brief every host dose correspond to a worker.
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version
* @date 2016-08-09
 */

package mini_spider

import (
	"github.com/PuerkitoBio/goquery"
	"github.com/PuerkitoBio/purell"
	"net/http"
	"net/url"
	"time"
)

type LogFlags uint

// Log levels for the library's logger
const (
	LogError   LogFlags = 1 << iota // error
	LogIgnored                      // filtered links
	LogInfo                         // info
	LogTrace                        // trace
	LogNone    LogFlags = 0
	LogAll     LogFlags = LogError | LogInfo | LogTrace
)

const (
	HEAD = true
	GET  = false
)

// Default headers
const (
	DefaultUserAgent      string = `Mozilla/5.0 (Windows NT 6.1; rv:15.0) mini_spider/1.0.0 Gecko/20120716 Firefox/15.0a2`
	DefaultRobotUserAgent string = `mini_spider (mini_spider v1.0.0)`
	DefaultMethodFlag     bool   = GET
)

// Default Worker options
const (
	DefaultEnqueueChanBuffer  int                       = 100
	DefaultHostBufferFactor   int                       = 10
	DefaultCrawlDelay         time.Duration             = 5 * time.Second
	DefaultWorkerIdleTTL      time.Duration             = 10 * time.Second
	DefaultNormalizationFlags purell.NormalizationFlags = purell.FlagsAllGreedy
	DefaultLogLevel           LogFlags                  = LogInfo
	DefaultMaxVisits          int                       = 0
	DefaultRedisDBAddr        string                    = "10.210.84.15:6379"
	DefaultRedisPasswd        string                    = "lishuo"
	DefaultLinkID             int                       = 3
	DefaultPageID             int                       = 4
)

// The Options available to control and customize the crawling process.
type Options struct {
	UserAgent             string        // UA
	RobotUserAgent        string        // Robot UA
	MaxVisits             int           // Maximum number of visitting, -1 means infinitely
	EnqueueChanBuffer     int           // the length of enqueue channel buffer
	HostBufferFactor      int           // the factor of host buffer
	CrawlDelay            time.Duration // Applied per host
	WorkerIdleTTL         time.Duration // timeout: worker automatically disappear after this time. -1: never disappear
	SaverIdleTTL          time.Duration // timeout: worker automatically disappear after this time. -1: never disappear
	SameHostOnly          bool          // 1. make length equal to the number of input seeds' host; 2. only allowed same host follow
	HeadBeforeGet         bool          // "GET" after "HEAD"
	URLNormalizationFlags purell.NormalizationFlags
	LogFlags              LogFlags      // level of logger
	LinkExpiration        time.Duration // link expiration
	PageExpiration        time.Duration // page expiration
	RedisDBAddr           string        // db address: host:port
	RedisPasswd           string        // redis passwd
	LinkID                int           // link db id
	PageID                int           // page db id
}

// Options' default constructor
func NewOptions() *Options {
	return &Options{
		DefaultUserAgent,
		DefaultRobotUserAgent,
		DefaultMaxVisits,
		DefaultEnqueueChanBuffer,
		DefaultHostBufferFactor,
		DefaultCrawlDelay,
		DefaultWorkerIdleTTL,
		DefaultSaverIdleTTL,
		true,
		false,
		DefaultNormalizationFlags,
		DefaultLogLevel,
		DefaultLinkExpiration,
		DefaultPageExpiration,
		DefaultRedisDBAddr,
		DefaultRedisPasswd,
		DefaultLinkID,
		DefaultPageID,
	}
}

// TODO 因为两者的数据集合可能差别很大，先初步分开存储，
// 理想上一起存储且条件应当是不影响读写性能
// Link
type Link struct {
	//URL       *url.URL // normalizedURL
	//SourceURL *url.URL // normalized SourceURL
	URL       string
	SourceURL string

	IsNew     bool        // is a new link
	State     interface{} // status code
	Intime    time.Time   // link found time
	Crawltime time.Time   // link crawl time
}

func NewEmptyLink() *Link {
	//return &Link{nil, nil, false, nil, time.Now(), time.Now()}
	return &Link{"", "", false, nil, time.Now(), time.Now()}
}

func NewLink(u string) *Link {
	/*
		if ul, err := url.Parse(u); err == nil {
			return &Link{ul, nil, false, nil, time.Now(), time.Now()}
		} else {
			return NewEmptyLink()
		}
	*/
	return &Link{u, "", false, nil, time.Now(), time.Now()}
}

// Page
type Page struct {
	//URL   *url.URL   // url
	URL string
	//Links []*url.URL // follow links
	Links []string // follow links

	Intime     time.Time // first in time
	Updatetime time.Time // update time
	Page       string    // page
	//Codetype
}

func NewPage(u string) *Page {
	return &Page{
		URL:        u,
		Links:      nil,
		Intime:     time.Now(),
		Updatetime: time.Now(),
		Page:       "",
	}
}

// url context
type URLContext struct {
	HeadBeforeGet bool              // "GET" or "HEAD"
	State         interface{}       // status code
	Headers       map[string]string // headers: include UA
	Crawltime     time.Time         // crawl time

	// Internal fields, available through getters
	url                 *url.URL
	normalizedURL       *url.URL
	sourceURL           *url.URL
	normalizedSourceURL *url.URL
	page                []byte // Page: web content
}

func NewURLContext(u interface{}) []*URLContext {
	return toURLContexts(u, nil, DefaultNormalizationFlags)
}

// Extension methods required to provide an extender instance.
type Actions interface {
	// Start, End, Error and Log are not related to a specific URL, so they don't
	// receive a URLContext struct.
	Start(interface{}) interface{}
	End(error)
	Error(*CrawlError)
	Log(LogFlags, LogFlags, string)

	// ComputeDelay is related to a Host only, not to a URLContext, although the FetchInfo
	// is related to a URLContext (holds a ctx field).
	ComputeDelay(string, *DelayInfo, *FetchInfo) time.Duration

	// All other extender methods are executed in the context of an URL, and thus
	// receive an URLContext struct as first argument.
	Fetch(*URLContext) (*http.Response, error)
	RequestGet(*URLContext, *http.Response) bool
	RequestRobots(*URLContext, string) ([]byte, bool)
	FetchedRobots(*URLContext, *http.Response)
	Filter(*URLContext, bool) bool
	Enqueued(*URLContext)
	Visit(*URLContext, *http.Response, *goquery.Document) (interface{}, bool)
	Visited(*URLContext, interface{})
	Disallowed(*URLContext)
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
	MergeLink(*Link, *Link) *Link
	MergePage(*Page, *Page) *Page
}

func NewDefaultActions() Actions {
	return &DefaultActions{}
}

//Crawler constructor with the specified extender object.
//func NewCrawler(ext Extender) *Crawler {
//return NewCrawlerWithOptions(NewOptions(ext))
//}
