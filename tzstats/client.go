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

	lru "github.com/hashicorp/golang-lru"
)

var (
	ClientVersion    = "0.14.0"
	DefaultLimit     = 50000
	DefaultCacheSize = 2048
	userAgent        = "tzstats-go/v" + ClientVersion
	DefaultClient    *Client
	IpfsClient       *Client
)

func init() {
	DefaultClient, _ = NewClient("https://api.tzstats.com", nil)
	IpfsClient, _ = NewClient("https://ipfs.tzstats.com", nil)
	IpfsClient.SetTimeout(60 * time.Second)
}

type Client struct {
	httpClient *http.Client
	params     Params
	cache      *lru.TwoQueueCache
	headers    http.Header
	UserAgent  string
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
		httpClient: httpClient,
		params:     params,
		cache:      cache,
		headers:    make(http.Header),
		UserAgent:  userAgent,
	}, nil
}

func (c *Client) DefaultHeaders() http.Header {
	return c.headers
}

func (c *Client) WithTLS(tc *tls.Config) *Client {
	c.httpClient.Transport.(*http.Transport).TLSClientConfig = tc
	return c
}

func (c *Client) UseScriptCache(cache *lru.TwoQueueCache) {
	c.cache = cache
}

func (c *Client) SetTimeout(d time.Duration) *Client {
	c.httpClient.Transport.(*http.Transport).ResponseHeaderTimeout = d
	c.httpClient.Timeout = d
	return c
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
		path = c.params.Url(path)
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
	headers.Set("User-Agent", c.UserAgent)

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
	log.Debugf("%s %s", method, path)
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
	log.Trace(newLogClosure(func() string {
		r, _ := httputil.DumpRequestOut(req.httpRequest, req.httpRequest.Header.Get("Content-Type") == "application/json")
		return string(r)
	}))

	resp, err := c.httpClient.Do(req.httpRequest)
	if err != nil {
		req.responseChan <- &response{err: err, request: req.String()}
		return
	}
	defer resp.Body.Close()

	log.Tracef("response: %s", newLogClosure(func() string {
		s, _ := httputil.DumpResponse(resp, isTextResponse(resp))
		return string(s)
	}))

	// process as stream when response interface is an io.Writer
	if resp.StatusCode == http.StatusOK && req.responseVal != nil {
		if stream, ok := req.responseVal.(io.Writer); ok {
			// log.Tracef("start streaming response")
			// forward stream
			_, err := io.Copy(stream, resp.Body)
			// close consumer if possible
			if closer, ok := req.responseVal.(io.WriteCloser); ok {
				// log.Tracef("closing stream after %d bytes", n)
				closer.Close()
			}
			// log.Tracef("response headers: %#v", resp.Header)
			// log.Tracef("response trailer: %#v", resp.Trailer)
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
