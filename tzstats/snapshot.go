// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"blockwatch.cc/tzgo/tezos"
)

type Snapshot struct {
	RowId        uint64        `json:"row_id"`
	Height       int64         `json:"height"`
	Cycle        int64         `json:"cycle"`
	IsSelected   bool          `json:"is_selected"`
	Timestamp    time.Time     `json:"time"`
	Index        int64         `json:"index"`
	Rolls        int64         `json:"rolls"`
	AccountId    uint64        `json:"account_id"`
	Address      tezos.Address `json:"address"`
	BakerId      uint64        `json:"baker_id"`
	Baker        tezos.Address `json:"baker"`
	IsBaker      bool          `json:"is_baker"`
	IsActive     bool          `json:"is_active"`
	Balance      float64       `json:"balance"`
	Delegated    float64       `json:"delegated"`
	NDelegations int64         `json:"n_delegations"`
	Since        int64         `json:"since"`
	SinceTime    time.Time     `json:"since_time"`
	columns      []string      `json:"-"`
}

type SnapshotList struct {
	Rows    []*Snapshot
	columns []string
}

func (l SnapshotList) Len() int {
	return len(l.Rows)
}

func (l SnapshotList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *SnapshotList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("SnapshotList: expected JSON array")
	}
	// log.Debugf("decode rights list from %d bytes", len(data))
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		r := &Snapshot{
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

func (s *Snapshot) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return s.UnmarshalJSONBrief(data)
	}
	type Alias *Snapshot
	return json.Unmarshal(data, Alias(s))
}

func (s *Snapshot) UnmarshalJSONBrief(data []byte) error {
	snap := Snapshot{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]interface{}, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return err
	}
	for i, v := range s.columns {
		f := unpacked[i]
		if f == nil {
			continue
		}
		switch v {
		case "row_id":
			snap.RowId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "height":
			snap.Height, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "cycle":
			snap.Cycle, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "is_selected":
			snap.IsSelected, err = strconv.ParseBool(f.(json.Number).String())
		case "time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				snap.Timestamp = time.Unix(0, ts*1000000).UTC()
			}
		case "index":
			snap.Index, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "rolls":
			snap.Rolls, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "address":
			snap.Address, err = tezos.ParseAddress(f.(string))
		case "account_id":
			snap.AccountId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "baker":
			snap.Baker, err = tezos.ParseAddress(f.(string))
		case "baker_id":
			snap.BakerId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "is_baker":
			snap.IsBaker, err = strconv.ParseBool(f.(json.Number).String())
		case "is_active":
			snap.IsActive, err = strconv.ParseBool(f.(json.Number).String())
		case "balance":
			snap.Balance, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "delegated":
			snap.Delegated, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "n_delegations":
			snap.NDelegations, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "since":
			snap.Since, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "since_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				snap.SinceTime = time.Unix(0, ts*1000000).UTC()
			}
		}
		if err != nil {
			return err
		}
	}
	*s = snap
	return nil
}

type SnapshotQuery struct {
	tableQuery
}

func (c *Client) NewSnapshotQuery() SnapshotQuery {
	tinfo, err := GetTypeInfo(&Snapshot{})
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.params.Copy(),
		Table:   "snapshot",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Columns: tinfo.Aliases(),
		Order:   OrderAsc,
		Filter:  make(FilterList, 0),
	}
	return SnapshotQuery{q}
}

func (q SnapshotQuery) Run(ctx context.Context) (*SnapshotList, error) {
	result := &SnapshotList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QuerySnapshots(ctx context.Context, filter FilterList, cols []string) (*SnapshotList, error) {
	q := c.NewSnapshotQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}
