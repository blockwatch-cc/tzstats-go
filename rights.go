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
	"time"

	"blockwatch.cc/tzgo/tezos"
)

type Right struct {
	RowId          uint64          `json:"row_id"`
	Height         int64           `json:"height"`
	Cycle          int64           `json:"cycle"`
	Timestamp      time.Time       `json:"time"`
	Type           tezos.RightType `json:"type"`
	Priority       int             `json:"priority"`
	Slots          []int           `json:"slots"`
	AccountId      uint64          `json:"account_id"`
	Address        tezos.Address   `json:"address"`
	IsUsed         bool            `json:"is_used"`
	IsLost         bool            `json:"is_lost"`
	IsStolen       bool            `json:"is_stolen"`
	IsMissed       bool            `json:"is_missed"`
	IsBondMiss     bool            `json:"is_bond_miss"`
	IsSeedRequired bool            `json:"is_seed_required"`
	IsSeedRevealed bool            `json:"is_seed_revealed"`
	columns        []string        `json:"-"`
}

func decodeBitVector(s string) []int {
	retval := make([]int, 0)
	buf, _ := hex.DecodeString(s)
	for idx, value := range buf {
		for i := 0; i < 8; i++ {
			if value&(1<<uint(i)) != 0 {
				retval = append(retval, idx*8+i)
			}
		}
	}
	return retval
}

type RightsList struct {
	Rows    []*Right
	columns []string
}

func (l RightsList) Len() int {
	return len(l.Rows)
}

func (l RightsList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *RightsList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Compare(data, []byte("null")) == 0 {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("RightsList: expected JSON array")
	}
	// log.Debugf("decode rights list from %d bytes", len(data))
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		r := &Right{
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

func (r *Right) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Compare(data, []byte("null")) == 0 {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return r.UnmarshalJSONBrief(data)
	}
	type Alias *Right
	return json.Unmarshal(data, Alias(r))
}

func (r *Right) UnmarshalJSONBrief(data []byte) error {
	right := Right{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]interface{}, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return err
	}
	for i, v := range r.columns {
		f := unpacked[i]
		if f == nil {
			continue
		}
		switch v {
		case "row_id":
			right.RowId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "height":
			right.Height, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "cycle":
			right.Cycle, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				right.Timestamp = time.Unix(0, ts*1000000).UTC()
			}
		case "type":
			right.Type = tezos.ParseRightType(f.(string))
		case "priority":
			right.Priority, err = strconv.Atoi(f.(json.Number).String())
		case "slots":
			// TODO: decode from hex string
			right.Slots = decodeBitVector(f.(string))
		case "account_id":
			right.AccountId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "address":
			right.Address, err = tezos.ParseAddress(f.(string))
		case "is_used":
			right.IsUsed, err = strconv.ParseBool(f.(json.Number).String())
		case "is_lost":
			right.IsLost, err = strconv.ParseBool(f.(json.Number).String())
		case "is_stolen":
			right.IsStolen, err = strconv.ParseBool(f.(json.Number).String())
		case "is_missed":
			right.IsMissed, err = strconv.ParseBool(f.(json.Number).String())
		case "is_bond_miss":
			right.IsBondMiss, err = strconv.ParseBool(f.(json.Number).String())
		case "is_seed_required":
			right.IsSeedRequired, err = strconv.ParseBool(f.(json.Number).String())
		case "is_seed_revealed":
			right.IsSeedRevealed, err = strconv.ParseBool(f.(json.Number).String())
		}
		if err != nil {
			return err
		}
	}
	*r = right
	return nil
}

type RightsQuery struct {
	tableQuery
}

func (c *Client) NewRightsQuery() RightsQuery {
	tinfo, err := GetTypeInfo(&Right{}, "")
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.params.Copy(),
		Table:   "rights",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Order:   OrderAsc,
		Columns: tinfo.Aliases(),
		Filter:  make(FilterList, 0),
	}
	return RightsQuery{q}
}

func (q RightsQuery) Run(ctx context.Context) (*RightsList, error) {
	result := &RightsList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryRights(ctx context.Context, filter FilterList, cols []string) (*RightsList, error) {
	q := c.NewRightsQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}
