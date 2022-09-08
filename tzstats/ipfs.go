// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"context"
	"io"
	"net/http"
	"strings"
)

func (c *Client) GetIpfsData(ctx context.Context, uri string, val interface{}) error {
	if strings.HasPrefix(uri, "ipfs://") {
		uri = "/ipfs/" + strings.TrimPrefix(uri, "ipfs://")
	}
	return c.get(ctx, uri, nil, val)
}

func (c *Client) GetIpfsImage(ctx context.Context, uri, mime string, w io.Writer) error {
	if strings.HasPrefix(uri, "ipfs://") {
		uri = "/ipfs/" + strings.TrimPrefix(uri, "ipfs://")
	}
	h := make(http.Header)
	h.Add("Accept", mime)
	return c.get(ctx, uri, h, w)
}
