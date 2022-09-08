// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

type Constant struct {
	RowId       uint64         `json:"row_id"`
	Address     tezos.ExprHash `json:"address"`
	CreatorId   uint64         `json:"creator_id"`
	Creator     tezos.Address  `json:"creator"`
	Height      int64          `json:"height"`
	Time        time.Time      `json:"time"`
	StorageSize int64          `json:"storage_size"`
	Value       micheline.Prim `json:"value"`
	Features    []string       `json:"features"`

	columns []string `json:"-"`
}

type ConstantList struct {
	Rows    []*Constant
	columns []string
}

func (l ConstantList) Len() int {
	return len(l.Rows)
}

func (l ConstantList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *ConstantList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("ConstantList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		r := &Constant{
			columns: l.columns,
		}
		if err := r.UnmarshalJSON(v); err != nil {
			return err
		}
		r.columns = nil
		l.Rows = append(l.Rows, r)
	}
	return nil
}

func (a *Constant) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return a.UnmarshalJSONBrief(data)
	}
	type Alias *Constant
	return json.Unmarshal(data, Alias(a))
}

func (c *Constant) UnmarshalJSONBrief(data []byte) error {
	cc := Constant{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]interface{}, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return err
	}
	for i, v := range c.columns {
		// var t int64
		f := unpacked[i]
		if f == nil {
			continue
		}
		switch v {
		case "row_id":
			cc.RowId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "address":
			cc.Address, err = tezos.ParseExprHash(f.(string))
		case "creator_id":
			cc.CreatorId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "creator":
			cc.Creator, err = tezos.ParseAddress(f.(string))
		case "height":
			cc.Height, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				cc.Time = time.Unix(0, ts*1000000).UTC()
			}
		case "storage_size":
			cc.StorageSize, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "value":
			var buf []byte
			buf, err = hex.DecodeString(f.(string))
			if err == nil {
				err = cc.Value.UnmarshalBinary(buf)
			}
		case "features":
			cc.Features = strings.Split(f.(string), ",")
		}
		if err != nil {
			return err
		}
	}
	*c = cc
	return nil
}

type ConstantParams struct {
	Params
}

func NewConstantParams() ConstantParams {
	return ConstantParams{NewParams()}
}

func (p ConstantParams) WithLimit(v uint) ConstantParams {
	p.Query.Set("limit", strconv.Itoa(int(v)))
	return p
}

func (p ConstantParams) WithOffset(v uint) ConstantParams {
	p.Query.Set("offset", strconv.Itoa(int(v)))
	return p
}

func (p ConstantParams) WithCursor(v uint64) ConstantParams {
	p.Query.Set("cursor", strconv.FormatUint(v, 10))
	return p
}

func (p ConstantParams) WithOrder(v OrderType) ConstantParams {
	p.Query.Set("order", string(v))
	return p
}

type ConstantQuery struct {
	tableQuery
}

func (c *Client) NewConstantQuery() ConstantQuery {
	tinfo, err := GetTypeInfo(&Constant{})
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.params.Copy(),
		Table:   "constant",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Order:   OrderAsc,
		Columns: tinfo.Aliases(),
		Filter:  make(FilterList, 0),
	}
	return ConstantQuery{q}
}

func (q ConstantQuery) Run(ctx context.Context) (*ConstantList, error) {
	result := &ConstantList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryConstants(ctx context.Context, filter FilterList, cols []string) (*ConstantList, error) {
	q := c.NewConstantQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}

func (c *Client) GetConstant(ctx context.Context, addr tezos.ExprHash, params ConstantParams) (*Constant, error) {
	cc := &Constant{}
	u := params.AppendQuery(fmt.Sprintf("/explorer/constant/%s", addr))
	if err := c.get(ctx, u, nil, cc); err != nil {
		return nil, err
	}
	return cc, nil
}
