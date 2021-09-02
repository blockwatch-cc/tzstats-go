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

type Bigmap struct {
	Contract        tezos.Address     `json:"contract"`
	BigmapId        int64             `json:"bigmap_id"`
	NUpdates        int64             `json:"n_updates"`
	NKeys           int64             `json:"n_keys"`
	AllocatedHeight int64             `json:"alloc_height"`
	AllocatedBlock  tezos.BlockHash   `json:"alloc_block"`
	AllocatedTime   time.Time         `json:"alloc_time"`
	UpdatedHeight   int64             `json:"update_height"`
	UpdatedBlock    tezos.BlockHash   `json:"update_block"`
	UpdatedTime     time.Time         `json:"update_time"`
	KeyType         micheline.Typedef `json:"key_type"`
	ValueType       micheline.Typedef `json:"value_type"`
	KeyTypePrim     micheline.Prim    `json:"key_type_prim"`
	ValueTypePrim   micheline.Prim    `json:"value_type_prim"`
}

func (b Bigmap) MakeKeyType() micheline.Type {
	return micheline.NewType(b.KeyTypePrim)
}

func (b Bigmap) MakeValueType() micheline.Type {
	return micheline.NewType(b.ValueTypePrim)
}

type BigmapRow struct {
	RowId        uint64          `json:"row_id"`
	Contract     tezos.Address   `json:"contract"`
	AccountId    uint64          `json:"account_id"`
	BigmapId     int64           `json:"bigmap_id"`
	NUpdates     int64           `json:"n_updates"`
	NKeys        int64           `json:"n_keys"`
	AllocHeight  int64           `json:"alloc_height"`
	AllocTime    time.Time       `json:"alloc_time"`
	AllocBlock   tezos.BlockHash `json:"alloc_block"`
	UpdateHeight int64           `json:"update_height"`
	UpdateTime   time.Time       `json:"update_time"`
	UpdateBlock  tezos.BlockHash `json:"update_block"`
	KeyType      string          `json:"key_type"`
	ValueType    string          `json:"value_type"`

	columns []string `json:"-"`
}

func (r BigmapRow) DecodeKeyType() (micheline.Type, error) {
	var t micheline.Type
	buf, err := hex.DecodeString(r.KeyType)
	if err != nil {
		return t, nil
	}
	if len(buf) == 0 {
		return t, io.ErrShortBuffer
	}
	err = t.UnmarshalBinary(buf)
	return t, err
}

func (r BigmapRow) DecodeValueType() (micheline.Type, error) {
	var t micheline.Type
	buf, err := hex.DecodeString(r.ValueType)
	if err != nil {
		return t, nil
	}
	if len(buf) == 0 {
		return t, io.ErrShortBuffer
	}
	err = t.UnmarshalBinary(buf)
	return t, err
}

type BigmapRowList struct {
	Rows    []*BigmapRow
	columns []string
}

func (l BigmapRowList) Len() int {
	return len(l.Rows)
}

func (l BigmapRowList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *BigmapRowList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Compare(data, []byte("null")) == 0 {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("BigmapRowList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		b := &BigmapRow{
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

func (b *BigmapRow) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Compare(data, []byte("null")) == 0 {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return b.UnmarshalJSONBrief(data)
	}
	type Alias *BigmapRow
	return json.Unmarshal(data, Alias(b))
}

func (b *BigmapRow) UnmarshalJSONBrief(data []byte) error {
	br := BigmapRow{}
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
		case "contract":
			br.Contract, err = tezos.ParseAddress(f.(string))
		case "account_id":
			br.AccountId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "bigmap_id":
			br.BigmapId, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_updates":
			br.NUpdates, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_keys":
			br.NKeys, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "alloc_height":
			br.AllocHeight, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "alloc_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				br.AllocTime = time.Unix(0, ts*1000000).UTC()
			}
		case "alloc_block":
			br.AllocBlock, err = tezos.ParseBlockHash(f.(string))
		case "update_height":
			br.UpdateHeight, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "update_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				br.UpdateTime = time.Unix(0, ts*1000000).UTC()
			}
		case "update_block":
			br.UpdateBlock, err = tezos.ParseBlockHash(f.(string))
		case "key_type":
			br.KeyType = f.(string)
		case "value_type":
			br.ValueType = f.(string)
		}
		if err != nil {
			return err
		}
	}
	*b = br
	return nil
}

type BigmapQuery struct {
	tableQuery
}

func (c *Client) NewBigmapQuery() BigmapQuery {
	tinfo, err := GetTypeInfo(&BigmapRow{}, "")
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.params.Copy(),
		Table:   "bigmaps",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Order:   OrderAsc,
		Columns: tinfo.Aliases(),
		Filter:  make(FilterList, 0),
	}
	return BigmapQuery{q}
}

func (q BigmapQuery) Run(ctx context.Context) (*BigmapRowList, error) {
	result := &BigmapRowList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryBigmaps(ctx context.Context, filter FilterList, cols []string) (*BigmapRowList, error) {
	q := c.NewBigmapQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}

func (c *Client) GetBigmap(ctx context.Context, id int64, params ContractParams) (*Bigmap, error) {
	b := &Bigmap{}
	u := params.AppendQuery(fmt.Sprintf("/explorer/bigmap/%d", id))
	if err := c.get(ctx, u, nil, b); err != nil {
		return nil, err
	}
	return b, nil
}
