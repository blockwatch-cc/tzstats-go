// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	if err := c.get(ctx, c.market.Url("/markets/tickers"), nil, &ticks); err != nil {
		return nil, err
	}
	return ticks, nil
}

func (c *Client) GetTicker(ctx context.Context, market, pair string) (*Ticker, error) {
	var tick Ticker
	u := c.market.Url(fmt.Sprintf("/markets/%s/%s/ticker", market, pair))
	if err := c.get(ctx, u, nil, &tick); err != nil {
		return nil, err
	}
	return &tick, nil
}

type Candle struct {
	Timestamp       time.Time `json:"time"`
	Open            float64   `json:"open"`
	High            float64   `json:"high"`
	Low             float64   `json:"low"`
	Close           float64   `json:"close"`
	Vwap            float64   `json:"vwap"`
	NTrades         int64     `json:"n_trades"`
	NBuy            int64     `json:"n_buy"`
	NSell           int64     `json:"n_sell"`
	VolumeBase      float64   `json:"vol_base"`
	VolumeQuote     float64   `json:"vol_quote"`
	VolumeBuyBase   float64   `json:"vol_buy_base"`
	VolumeBuyQuote  float64   `json:"vol_buy_quote"`
	VolumeSellBase  float64   `json:"vol_sell_base"`
	VolumeSellQuote float64   `json:"vol_sell_quote"`

	columns []string
}

func (c *Candle) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return c.UnmarshalJSONBrief(data)
	}
	type alias *Candle
	return json.Unmarshal(data, alias(c))
}

func (c *Candle) UnmarshalJSONBrief(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]interface{}, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return err
	}
	for i, v := range c.columns {
		f := unpacked[i]
		if f == nil {
			continue
		}
		switch v {
		case "time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				c.Timestamp = time.Unix(0, ts*1000000).UTC()
			}
		case "open":
			c.Open, err = f.(json.Number).Float64()
		case "high":
			c.High, err = f.(json.Number).Float64()
		case "low":
			c.Low, err = f.(json.Number).Float64()
		case "close":
			c.Close, err = f.(json.Number).Float64()
		case "vwap":
			c.Vwap, err = f.(json.Number).Float64()
		case "n_trades":
			c.NTrades, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_buy":
			c.NBuy, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_sell":
			c.NSell, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "vol_base":
			c.VolumeBase, err = f.(json.Number).Float64()
		case "vol_quote":
			c.VolumeQuote, err = f.(json.Number).Float64()
		case "vol_buy_base":
			c.VolumeBuyBase, err = f.(json.Number).Float64()
		case "vol_buy_quote":
			c.VolumeBuyQuote, err = f.(json.Number).Float64()
		case "vol_sell_base":
			c.VolumeSellBase, err = f.(json.Number).Float64()
		case "vol_sell_quote":
			c.VolumeSellQuote, err = f.(json.Number).Float64()
		}
	}
	if err != nil {
		return err
	}
	return nil
}

type CandleList struct {
	Columns []string
	Rows    []Candle
}

func (l CandleList) Len() int {
	return len(l.Rows)
}

func (l *CandleList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] == '{' {
		type alias *CandleList
		return json.Unmarshal(data, alias(l))
	}
	if data[0] != '[' {
		return fmt.Errorf("CandleList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		c := Candle{
			columns: l.Columns,
		}
		if err := c.UnmarshalJSON(v); err != nil {
			return err
		}
		c.columns = nil
		l.Rows = append(l.Rows, c)
	}
	return nil
}

func (l CandleList) AsOf(t time.Time) (c Candle) {
	// when collapsing the timestamp is set to the beginning of the
	// aggregation interval (e.g. timestamp = Jun 3 means all day June 3)
	idx := sort.Search(l.Len(), func(i int) bool { return !l.Rows[i].Timestamp.Before(t) })
	if idx > 0 && idx < l.Len() {
		c = l.Rows[idx-1]
	} else {
		c = l.Rows[l.Len()-1]
	}
	return
}

type FillMode string

const (
	FillModeInvalid FillMode = ""
	FillModeNone    FillMode = "none"
	FillModeNull    FillMode = "null"
	FillModeLast    FillMode = "last"
	FillModeLinear  FillMode = "linear"
	FillModeZero    FillMode = "zero"
)

const (
	Collapse1m = 1 * time.Minute
	Collapse1h = 1 * time.Hour
	Collapse1d = 24 * time.Hour
	Collapse1w = 7 * 24 * time.Hour
)

type CandleArgs struct {
	Market   string
	Pair     string
	Collapse time.Duration
	Fill     FillMode
	Columns  []string
	From     time.Time
	To       time.Time
	Limit    int
}

func (c CandleArgs) Url() string {
	p := NewParams()
	if c.Limit > 0 && p.Query.Get("limit") == "" {
		p.Query.Set("limit", strconv.Itoa(c.Limit))
	}
	if len(c.Columns) > 0 && p.Query.Get("columns") == "" {
		p.Query.Set("columns", strings.Join(c.Columns, ","))
	}
	if len(c.Fill) > 0 && p.Query.Get("fill") == "" {
		p.Query.Set("fill", string(c.Fill))
	}
	if c.Collapse > 0 && p.Query.Get("collapse") == "" {
		p.Query.Set("collapse", c.Collapse.String())
	}
	if !c.From.IsZero() && p.Query.Get("start_date") == "" {
		p.Query.Set("start_date", c.From.Format(time.RFC3339))
	}
	if !c.To.IsZero() && p.Query.Get("end_date") == "" {
		p.Query.Set("end_date", c.To.Format(time.RFC3339))
	}
	return p.Url("/series/" + c.Market + "/" + c.Pair + "/ohlcv")
}

func (c *Client) ListCandles(ctx context.Context, args CandleArgs) (*CandleList, error) {
	if len(args.Columns) == 0 {
		tinfo, err := GetTypeInfo(&Candle{})
		if err != nil {
			panic(err)
		}
		args.Columns = tinfo.FilteredAliases("noseries")
	}
	resp := &CandleList{
		Rows:    make([]Candle, 0),
		Columns: args.Columns,
	}
	if err := c.get(ctx, c.market.Url(args.Url()), nil, resp); err != nil {
		return nil, err
	}
	return resp, nil
}
