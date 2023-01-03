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
	"math/big"
	"strconv"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

type BigmapValue struct {
	Key       MultiKey        `json:"key"`
	Hash      tezos.ExprHash  `json:"hash"`
	Meta      *BigmapMeta     `json:"meta,omitempty"`
	Value     interface{}     `json:"value,omitempty"`
	Height    int64           `json:"height"`
	Time      time.Time       `json:"time"`
	KeyPrim   *micheline.Prim `json:"key_prim,omitempty"`
	ValuePrim *micheline.Prim `json:"value_prim,omitempty"`
}

type BigmapMeta struct {
	Contract     tezos.Address `json:"contract"`
	BigmapId     int64         `json:"bigmap_id"`
	UpdateTime   time.Time     `json:"time"`
	UpdateHeight int64         `json:"height"`
	UpdateOp     tezos.OpHash  `json:"op"`
	Sender       tezos.Address `json:"sender"`
	Source       tezos.Address `json:"source"`
}

func (v BigmapValue) Has(path string) bool {
	return hasPath(v.Value, path)
}

func (v BigmapValue) GetString(path string) (string, bool) {
	return getPathString(v.Value, path)
}

func (v BigmapValue) GetInt64(path string) (int64, bool) {
	return getPathInt64(v.Value, path)
}

func (v BigmapValue) GetBig(path string) (*big.Int, bool) {
	return getPathBig(v.Value, path)
}

func (v BigmapValue) GetZ(path string) (tezos.Z, bool) {
	return getPathZ(v.Value, path)
}

func (v BigmapValue) GetTime(path string) (time.Time, bool) {
	return getPathTime(v.Value, path)
}

func (v BigmapValue) GetAddress(path string) (tezos.Address, bool) {
	return getPathAddress(v.Value, path)
}

func (v BigmapValue) GetValue(path string) (interface{}, bool) {
	return getPathValue(v.Value, path)
}

func (v BigmapValue) Walk(path string, fn ValueWalkerFunc) error {
	val := v.Value
	if len(path) > 0 {
		var ok bool
		val, ok = getPathValue(val, path)
		if !ok {
			return nil
		}
	}
	return walkValueMap(path, val, fn)
}

func (v BigmapValue) Unmarshal(val interface{}) error {
	buf, _ := json.Marshal(v.Value)
	return json.Unmarshal(buf, val)
}

type BigmapValueRow struct {
	RowId    uint64         `json:"row_id"`
	BigmapId int64          `json:"bigmap_id"`
	Height   int64          `json:"height"`
	Time     time.Time      `json:"time"`
	KeyId    uint64         `json:"key_id"`
	Hash     tezos.ExprHash `json:"hash,omitempty"`
	Key      string         `json:"key,omitempty"`
	Value    string         `json:"value,omitempty"`

	columns []string `json:"-"`
}

func (r BigmapValueRow) DecodeKey(typ micheline.Type) (micheline.Key, error) {
	buf, err := hex.DecodeString(r.Key)
	if err != nil {
		return micheline.Key{}, err
	}
	if len(buf) == 0 {
		return micheline.Key{}, io.ErrShortBuffer
	}
	return micheline.DecodeKey(typ, buf)
}

func (r BigmapValueRow) DecodeValue(typ micheline.Type) (micheline.Value, error) {
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

type BigmapValueRowList struct {
	Rows    []*BigmapValueRow
	columns []string
}

func (l BigmapValueRowList) Len() int {
	return len(l.Rows)
}

func (l BigmapValueRowList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *BigmapValueRowList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("BigmapValueRowList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		b := &BigmapValueRow{
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

func (b *BigmapValueRow) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return b.UnmarshalJSONBrief(data)
	}
	type Alias *BigmapValueRow
	return json.Unmarshal(data, Alias(b))
}

func (b *BigmapValueRow) UnmarshalJSONBrief(data []byte) error {
	br := BigmapValueRow{}
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
		case "key_id":
			br.KeyId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "key_hash":
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

type BigmapValueQuery struct {
	tableQuery
}

func (c *Client) NewBigmapValueQuery() BigmapValueQuery {
	tinfo, err := GetTypeInfo(&BigmapValueRow{})
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.base.Copy(),
		Table:   "bigmap_values",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Order:   OrderAsc,
		Columns: tinfo.Aliases(),
		Filter:  make(FilterList, 0),
	}
	return BigmapValueQuery{q}
}

func (q BigmapValueQuery) Run(ctx context.Context) (*BigmapValueRowList, error) {
	result := &BigmapValueRowList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryBigmapValues(ctx context.Context, filter FilterList, cols []string) (*BigmapValueRowList, error) {
	q := c.NewBigmapValueQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}

func (c *Client) GetBigmapValue(ctx context.Context, id int64, key string, params ContractParams) (*BigmapValue, error) {
	v := &BigmapValue{}
	u := params.AppendQuery(fmt.Sprintf("/explorer/bigmap/%d/%s", id, key))
	if err := c.get(ctx, u, nil, v); err != nil {
		return nil, err
	}
	return v, nil
}

func (c *Client) ListBigmapValues(ctx context.Context, id int64, params ContractParams) ([]BigmapValue, error) {
	vals := make([]BigmapValue, 0)
	u := params.AppendQuery(fmt.Sprintf("/explorer/bigmap/%d/values", id))
	if err := c.get(ctx, u, nil, &vals); err != nil {
		return nil, err
	}
	return vals, nil
}
