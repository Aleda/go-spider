/**
* @file urlcontext.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version 1.0.0
* @date 2016-08-09
 */

package mini_spider

import (
	"net/url"
)

func (uc *URLContext) URL() *url.URL {
	return uc.url
}

func (uc *URLContext) NormalizedURL() *url.URL {
	return uc.normalizedURL
}

func (uc *URLContext) SourceURL() *url.URL {
	return uc.sourceURL
}

func (uc *URLContext) NormalizedSourceURL() *url.URL {
	return uc.normalizedSourceURL
}

func (uc *URLContext) IsRobotsURL() bool {
	return isRobotsURL(uc.normalizedURL)
}

func (uc *URLContext) getRobotsURLCtx() (*URLContext, error) {
	robUrl, err := uc.normalizedURL.Parse(robotsTxtPath)
	if err != nil {
		return nil, err
	}
	return &URLContext{
		HeadBeforeGet:       false, // Never request HEAD before GET for robots.txt
		State:               nil,   // Always nil state
		Headers:             nil,
		url:                 robUrl,
		normalizedURL:       robUrl,       // Normalized is same as raw
		sourceURL:           uc.sourceURL, // Source and normalized source is same as for current context
		normalizedSourceURL: uc.normalizedSourceURL,
		page:                nil,
	}, nil
}
