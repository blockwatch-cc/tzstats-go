// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

type BigmapUpdate struct {
	BigmapValue
	Action        micheline.DiffAction `json:"action"`
	KeyType       *micheline.Typedef   `json:"key_type,omitempty"`
	ValueType     *micheline.Typedef   `json:"value_type,omitempty"`
	KeyTypePrim   *micheline.Prim      `json:"key_type_prim,omitempty"`
	ValueTypePrim *micheline.Prim      `json:"value_type_prim,omitempty"`
	BigmapId      int64                `json:"bigmap_id"`
	SourceId      int64                `json:"source_big_map,omitempty"`
	DestId        int64                `json:"destination_big_map,omitempty"`
}

type BigmapUpdateRow struct {
	RowId    uint64               `json:"row_id"`
	BigmapId int64                `json:"bigmap_id"`
	Action   micheline.DiffAction `json:"action"`
	KeyId    uint64               `json:"key_id"`
	Hash     tezos.ExprHash       `json:"hash,omitempty"`
	Key      string               `json:"key,omitempty"`
	Value    string               `json:"value,omitempty"`
	Height   int64                `json:"height"`
	Time     time.Time            `json:"time"`

	columns []string `json:"-"`
}

// Alloc/Copy only
func (r BigmapUpdateRow) DecodeKeyType() (micheline.Type, error) {
	switch r.Action {
	case micheline.DiffActionAlloc, micheline.DiffActionCopy:
	default:
		return micheline.Type{}, fmt.Errorf("no type info on bigmap %s", r.Action)
	}
	buf, err := hex.DecodeString(r.Key)
	if err != nil {
		return micheline.Type{}, nil
	}
	if len(buf) == 0 {
		return micheline.Type{}, io.ErrShortBuffer
	}
	t := micheline.Type{}
	err = t.UnmarshalBinary(buf)
	return t, err
}

// Alloc/Copy only
func (r BigmapUpdateRow) DecodeValueType() (micheline.Type, error) {
	switch r.Action {
	case micheline.DiffActionAlloc, micheline.DiffActionCopy:
	default:
		return micheline.Type{}, fmt.Errorf("no type info on bigmap %s", r.Action)
	}
	buf, err := hex.DecodeString(r.Value)
	if err != nil {
		return micheline.Type{}, nil
	}
	if len(buf) == 0 {
		return micheline.Type{}, io.ErrShortBuffer
	}
	t := micheline.Type{}
	err = t.UnmarshalBinary(buf)
	return t, err
}

// Update/Remove only
func (r BigmapUpdateRow) DecodeKey(typ micheline.Type) (micheline.Key, error) {
	switch r.Action {
	case micheline.DiffActionUpdate, micheline.DiffActionRemove:
	default:
		return micheline.Key{}, fmt.Errorf("no key on bigmap %s", r.Action)
	}

	buf, err := hex.DecodeString(r.Key)
	if err != nil {
		return micheline.Key{}, err
	}
	if len(buf) == 0 {
		return micheline.Key{}, io.ErrShortBuffer
	}
	return micheline.DecodeKey(typ, buf)
}

// Update only
func (r BigmapUpdateRow) DecodeValue(typ micheline.Type) (micheline.Value, error) {
	if r.Action != micheline.DiffActionUpdate {
		return micheline.Value{}, fmt.Errorf("no value on bigmap %s", r.Action)
	}
	v := micheline.NewValue(typ, micheline.Prim{})
	buf, err := hex.DecodeString(r.Value)
	if err != nil {
		return v, err
	}
	if len(buf) == 0 {
		return v, io.ErrShortBuffer
	}
	err = v.Decode(buf)
	return v, err
}

type BigmapUpdateRowList struct {
	Rows    []*BigmapUpdateRow
	columns []string
}

func (l BigmapUpdateRowList) Len() int {
	return len(l.Rows)
}

func (l BigmapUpdateRowList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *BigmapUpdateRowList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("BigmapUpdateRowList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		b := &BigmapUpdateRow{
			columns: l.columns,
		}
		if err := b.UnmarshalJSON(v); err != nil {
			return err
		}
		b.columns = nil
		l.Rows = append(l.Rows, b)
	}
	return nil
}

func (b *BigmapUpdateRow) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return b.UnmarshalJSONBrief(data)
	}
	type Alias *BigmapUpdateRow
	return json.Unmarshal(data, Alias(b))
}

func (b *BigmapUpdateRow) UnmarshalJSONBrief(data []byte) error {
	br := BigmapUpdateRow{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]interface{}, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return err
	}
	for i, v := range b.columns {
		f := unpacked[i]
		if f == nil {
			continue
		}
		switch v {
		case "row_id":
			br.RowId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "bigmap_id":
			br.BigmapId, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "action":
			br.Action, err = micheline.ParseDiffAction(f.(string))
		case "key_id":
			br.KeyId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "hash":
			br.Hash, err = tezos.ParseExprHash(f.(string))
		case "key":
			br.Key = f.(string)
		case "value":
			br.Value = f.(string)
		case "height":
			br.Height, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				br.Time = time.Unix(0, ts*1000000).UTC()
			}
		}
		if err != nil {
			return err
		}
	}
	*b = br
	return nil
}

type BigmapUpdateQuery struct {
	tableQuery
}

func (c *Client) NewBigmapUpdateQuery() BigmapUpdateQuery {
	tinfo, err := GetTypeInfo(&BigmapUpdateRow{})
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.params.Copy(),
		Table:   "bigmap_updates",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Order:   OrderAsc,
		Columns: tinfo.Aliases(),
		Filter:  make(FilterList, 0),
	}
	return BigmapUpdateQuery{q}
}

func (q BigmapUpdateQuery) Run(ctx context.Context) (*BigmapUpdateRowList, error) {
	result := &BigmapUpdateRowList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryBigmapUpdates(ctx context.Context, filter FilterList, cols []string) (*BigmapUpdateRowList, error) {
	q := c.NewBigmapUpdateQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}

func (c *Client) ListBigmapUpdates(ctx context.Context, id int64, params ContractParams) ([]BigmapUpdate, error) {
	upd := make([]BigmapUpdate, 0)
	u := params.AppendQuery(fmt.Sprintf("/explorer/bigmap/%d/updates", id))
	if err := c.get(ctx, u, nil, &upd); err != nil {
		return nil, err
	}
	return upd, nil
}

func (c *Client) ListBigmapKeyUpdates(ctx context.Context, id int64, key string, params ContractParams) ([]BigmapUpdate, error) {
	upd := make([]BigmapUpdate, 0)
	u := params.AppendQuery(fmt.Sprintf("/explorer/bigmap/%d/%s/updates", id, key))
	if err := c.get(ctx, u, nil, &upd); err != nil {
		return nil, err
	}
	return upd, nil
}
