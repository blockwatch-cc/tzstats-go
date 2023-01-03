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

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

type Event struct {
	// table API only
	RowId     uint64 `json:"row_id"`
	AccountId uint64 `json:"account_id"`
	Height    int64  `json:"height"`
	OpId      uint64 `json:"op_id"`

	// table and explorer API
	Contract tezos.Address  `json:"contract"`
	Type     micheline.Prim `json:"type"`
	Payload  micheline.Prim `json:"payload"`
	Tag      string         `json:"tag"`
	TypeHash string         `json:"type_hash"`

	columns []string `json:"-"`
}

type EventList struct {
	Rows    []*Event
	columns []string
}

func (l EventList) Len() int {
	return len(l.Rows)
}

func (l EventList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *EventList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("EventList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		r := &Event{
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

func (a *Event) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return a.UnmarshalJSONBrief(data)
	}
	type Alias *Event
	return json.Unmarshal(data, Alias(a))
}

func (e *Event) UnmarshalJSONBrief(data []byte) error {
	ev := Event{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]interface{}, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return err
	}
	for i, v := range e.columns {
		f := unpacked[i]
		if f == nil {
			continue
		}
		switch v {
		case "row_id":
			ev.RowId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "account_id":
			ev.AccountId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "height":
			ev.Height, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "op_id":
			ev.OpId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "contract":
			ev.Contract, err = tezos.ParseAddress(f.(string))
		case "type":
			var buf []byte
			if buf, err = hex.DecodeString(f.(string)); err == nil && len(buf) > 0 {
				err = ev.Type.UnmarshalBinary(buf)
			}
		case "payload":
			var buf []byte
			if buf, err = hex.DecodeString(f.(string)); err == nil && len(buf) > 0 {
				err = ev.Payload.UnmarshalBinary(buf)
			}
		case "tag":
			ev.Tag = f.(string)
		case "type_hash":
			ev.TypeHash = f.(string)
		}
		if err != nil {
			return err
		}
	}
	*e = ev
	return nil
}

type EventQuery struct {
	tableQuery
}

func (c *Client) NewEventQuery() EventQuery {
	tinfo, err := GetTypeInfo(&Event{})
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.base.Copy(),
		Table:   "event",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Order:   OrderAsc,
		Columns: tinfo.Aliases(),
		Filter:  make(FilterList, 0),
	}
	return EventQuery{q}
}

func (q EventQuery) Run(ctx context.Context) (*EventList, error) {
	result := &EventList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryEvents(ctx context.Context, filter FilterList, cols []string) (*EventList, error) {
	q := c.NewEventQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}
