// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/echa/log"
	lru "github.com/hashicorp/golang-lru"
)

var (
	ClientVersion    = "0.15.0"
	DefaultLimit     = 50000
	DefaultCacheSize = 2048
	userAgent        = "tzstats-go/v" + ClientVersion
	DefaultClient    *Client
	IpfsClient       *Client
)

func init() {
	DefaultClient, _ = NewClient("https://api.tzstats.com", nil)
	IpfsClient, _ = NewClient("https://ipfs.tzstats.com", nil)
	IpfsClient.WithTimeout(60 * time.Second)
}

type Client struct {
	transport  *http.Client
	log        log.Logger
	base       Params
	market     Params
	cache      *lru.TwoQueueCache
	headers    http.Header
	userAgent  string
	numRetries int
	retryDelay time.Duration
}

func NewClient(url string, httpClient *http.Client) (*Client, error) {
	params, err := ParseParams(url)
	if err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          10,
				MaxConnsPerHost:       10,
				IdleConnTimeout:       30 * time.Second,
				DisableCompression:    false,
				TLSHandshakeTimeout:   5 * time.Second,
				ResponseHeaderTimeout: 10 * time.Second,
			},
			Timeout: 60 * time.Second,
		}
	}
	sz := DefaultCacheSize
	if sz < 2 {
		sz = 2
	}
	cache, _ := lru.New2Q(sz)
	return &Client{
		transport:  httpClient,
		log:        defaultLog,
		base:       params,
		market:     params,
		cache:      cache,
		headers:    make(http.Header),
		userAgent:  userAgent,
		numRetries: 0,
		retryDelay: 0,
	}, nil
}

func (c *Client) DefaultHeaders() http.Header {
	return c.headers
}

func (c *Client) WithHeader(key, value string) *Client {
	c.headers.Set(key, value)
	return c
}

func (c *Client) WithUserAgent(s string) *Client {
	c.userAgent = s
	return c
}

func (c *Client) WithApiKey(s string) *Client {
	if s != "" {
		c.headers.Set("X-Api-Key", s)
	} else {
		c.headers.Del("X-Api-Key")
	}
	return c
}

func (c *Client) WithMarketUrl(url string) *Client {
	if params, err := ParseParams(url); err == nil {
		c.market = params
	}
	return c
}

func (c *Client) WithTLS(tc *tls.Config) *Client {
	c.transport.Transport.(*http.Transport).TLSClientConfig = tc
	return c
}

func (c *Client) WithTimeout(d time.Duration) *Client {
	c.transport.Transport.(*http.Transport).ResponseHeaderTimeout = d
	c.transport.Timeout = d
	return c
}

func (c *Client) WithRetry(num int, delay time.Duration) *Client {
	c.numRetries = num
	if num < 0 {
		c.numRetries = int(^uint(0)>>1) - 1 // max int - 1
	}
	c.retryDelay = delay
	return c
}

func (c *Client) WithLogger(log log.Logger) *Client {
	c.log = log
	return c
}

func (c *Client) WithCacheSize(sz int) *Client {
	if sz < 2 {
		sz = 2
	}
	cache, _ := lru.New2Q(sz)
	c.cache = cache
	return c
}

func (c *Client) UseScriptCache(cache *lru.TwoQueueCache) {
	c.cache = cache
}

func (c Client) Retries() int {
	return c.numRetries
}

func (c Client) RetryDelay() time.Duration {
	return c.retryDelay
}

func (c *Client) get(ctx context.Context, path string, headers http.Header, result interface{}) error {
	return c.call(ctx, http.MethodGet, path, headers, nil, result)
}

func (c *Client) post(ctx context.Context, path string, headers http.Header, data, result interface{}) error {
	return c.call(ctx, http.MethodPost, path, headers, data, result)
}

func (c *Client) put(ctx context.Context, path string, headers http.Header, data, result interface{}) error {
	return c.call(ctx, http.MethodPut, path, headers, data, result)
}

func (c *Client) delete(ctx context.Context, path string, headers http.Header) error {
	return c.call(ctx, http.MethodDelete, path, headers, nil, nil)
}

func (c *Client) Async(ctx context.Context, path string, headers http.Header, result interface{}) FutureResult {
	return c.callAsync(ctx, http.MethodGet, path, headers, nil, result)
}

func (c *Client) call(ctx context.Context, method, path string, headers http.Header, data, result interface{}) error {
	return c.callAsync(ctx, method, path, headers, data, result).Receive(ctx)
}

func (c *Client) callAsync(ctx context.Context, method, path string, headers http.Header, data, result interface{}) FutureResult {
	if !strings.HasPrefix(path, "http") {
		path = c.base.Url(path)
	}

	req, err := c.newRequest(ctx, method, path, headers, data, result)
	if err != nil {
		return newFutureError(err)
	}

	responseChan := make(chan *response, 1)
	c.handleRequest(&request{
		httpRequest:     req,
		responseVal:     result,
		responseHeaders: headers,
		responseChan:    responseChan,
	})

	return responseChan
}

func (c *Client) newRequest(ctx context.Context, method, path string, headers http.Header, data, result interface{}) (*http.Request, error) {
	// prepare headers
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("User-Agent", c.userAgent)

	// copy default headers
	for n, v := range c.headers {
		for _, vv := range v {
			headers.Add(n, vv)
		}
	}

	// prepare POST/PUT/PATCH payload
	var body io.Reader
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		body = bytes.NewBuffer(b)
		if headers.Get("Content-Type") == "" {
			headers.Set("Content-Type", "application/json")
		}
	}

	if result != nil && headers.Get("Accept") == "" {
		headers.Set("Accept", "application/json")
	}

	// create http request
	c.log.Debugf("%s %s", method, path)
	req, err := http.NewRequest(method, path, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	// add content-type header to POST, PUT, PATCH
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		headers.Del("Content-Type")
	}

	// add all passed in headers
	for n, v := range headers {
		if strings.ToLower(n) == "host" {
			req.Host = v[0]
			continue
		}
		for _, vv := range v {
			req.Header.Add(n, vv)
		}
	}

	return req, nil
}

// handleRequest executes the passed HTTP request, reading the
// result, unmarshalling it, and delivering the unmarshalled result to the
// provided response channel.
func (c *Client) handleRequest(req *request) {
	// only dump content-type application/json
	c.log.Trace(newLogClosure(func() string {
		r, _ := httputil.DumpRequestOut(req.httpRequest, req.httpRequest.Header.Get("Content-Type") == "application/json")
		return string(r)
	}))

	var (
		resp *http.Response
		err  error
	)
	for retries := c.numRetries + 1; retries > 0; retries-- {
		resp, err = c.transport.Do(req.httpRequest)
		if err == nil {
			break
		}
		if !isNetError(err) {
			break
		}
		select {
		case <-req.httpRequest.Context().Done():
			req.responseChan <- &response{
				err:     req.httpRequest.Context().Err(),
				request: req.String(),
			}
			return
		case <-time.After(c.retryDelay):
			// continue
		}
	}
	if err != nil {
		req.responseChan <- &response{err: err, request: req.String()}
		return
	}
	defer resp.Body.Close()

	c.log.Tracef("response: %s", newLogClosure(func() string {
		s, _ := httputil.DumpResponse(resp, isTextResponse(resp))
		return string(s)
	}))

	// process as stream when response interface is an io.Writer
	if resp.StatusCode == http.StatusOK && req.responseVal != nil {
		if stream, ok := req.responseVal.(io.Writer); ok {
			// c.log.Tracef("start streaming response")
			// forward stream
			_, err := io.Copy(stream, resp.Body)
			// close consumer if possible
			if closer, ok := req.responseVal.(io.WriteCloser); ok {
				// c.log.Tracef("closing stream after %d bytes", n)
				closer.Close()
			}
			// c.log.Tracef("response headers: %#v", resp.Header)
			// c.log.Tracef("response trailer: %#v", resp.Trailer)
			req.responseChan <- &response{
				status:  resp.StatusCode,
				request: req.String(),
				headers: mergeHeaders(req.responseHeaders, resp.Header, resp.Trailer),
				err:     err,
			}
			return
		}
	}

	// non-stream handling below

	// Read the raw bytes
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		req.responseChan <- &response{
			status:  resp.StatusCode,
			request: req.String(),
			headers: mergeHeaders(req.responseHeaders, resp.Header, resp.Trailer),
			err:     fmt.Errorf("reading reply: %w", err),
		}
		return
	}

	// on failure, return error and response (some API's send specific
	// error codes as details which we cannot parse here; some other APIs
	// even send 5xx error codes to signal non-error situations)
	if resp.StatusCode >= 400 {
		if resp.StatusCode == 429 {
			// TODO: read rate limit header
			wait := 5 * time.Second
			err = newRateLimitError(wait, resp)
		} else {
			err = newHttpError(resp, respBytes, req.String())
		}
		req.responseChan <- &response{
			status:  resp.StatusCode,
			request: req.String(),
			headers: mergeHeaders(req.responseHeaders, resp.Header, resp.Trailer),
			result:  respBytes,
			err:     err,
		}
		return
	}

	// unmarshal any JSON response
	isJson := strings.Contains(resp.Header.Get("Content-Type"), "application/json")

	// do this even if the response looks like JSON
	isJson = isJson || bytes.HasPrefix(respBytes, []byte("{")) || bytes.HasPrefix(respBytes, []byte("["))

	if isJson && req.responseVal != nil && (resp.ContentLength > 0 || resp.ContentLength == -1) {
		if err = json.Unmarshal(respBytes, req.responseVal); err == nil {
			req.responseChan <- &response{
				status:  resp.StatusCode,
				request: req.String(),
				headers: mergeHeaders(req.responseHeaders, resp.Header, resp.Trailer),
				err:     nil,
			}
			return
		}
		err = fmt.Errorf("unmarshaling reply: %w", err)
	}
	req.responseChan <- &response{
		status:  resp.StatusCode,
		request: req.String(),
		headers: mergeHeaders(req.responseHeaders, resp.Header, resp.Trailer),
		result:  respBytes,
		err:     err,
	}
}
