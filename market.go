// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"context"
	"time"
)

type Ticker struct {
	Pair        string    `json:"pair"`
	Base        string    `json:"base"`
	Quote       string    `json:"quote"`
	Exchange    string    `json:"exchange"`
	Open        float64   `json:"open"`
	High        float64   `json:"high"`
	Low         float64   `json:"low"`
	Last        float64   `json:"last"`
	Change      float64   `json:"change"`
	Vwap        float64   `json:"vwap"`
	NTrades     int64     `json:"n_trades"`
	VolumeBase  float64   `json:"volume_base"`
	VolumeQuote float64   `json:"volume_quote"`
	Time        time.Time `json:"timestamp"`
}

func (c *Client) GetTickers(ctx context.Context) ([]Ticker, error) {
	ticks := make([]Ticker, 0)
	if err := c.get(ctx, "/markets/tickers", nil, &ticks); err != nil {
		return nil, err
	}
	return ticks, nil
}
