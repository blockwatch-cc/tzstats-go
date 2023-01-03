// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

type Contract struct {
	RowId         uint64               `json:"row_id,omitempty"`
	AccountId     uint64               `json:"account_id,omitempty"`
	Address       tezos.Address        `json:"address"`
	CreatorId     uint64               `json:"creator_id,omitempty"`
	Creator       tezos.Address        `json:"creator"`
	BakerId       uint64               `json:"baker_id,omitempty" tzstats:"notable"`
	Baker         tezos.Address        `json:"baker"              tzstats:"notable"`
	FirstSeen     int64                `json:"first_seen"`
	LastSeen      int64                `json:"last_seen"`
	FirstSeenTime time.Time            `json:"first_seen_time"`
	LastSeenTime  time.Time            `json:"last_seen_time"`
	StorageSize   int64                `json:"storage_size"`
	StoragePaid   int64                `json:"storage_paid"`
	TotalFeesUsed float64              `json:"total_fees_used"    tzstats:"notable"`
	Script        *micheline.Script    `json:"script,omitempty"`
	Storage       *micheline.Prim      `json:"storage,omitempty"`
	InterfaceHash string               `json:"iface_hash"`
	CodeHash      string               `json:"code_hash"`
	StorageHash   string               `json:"storage_hash"`
	Features      []string             `json:"features"`
	Interfaces    []string             `json:"interfaces"`
	CallStats     map[string]int       `json:"call_stats"`
	NCallsIn      int                  `json:"n_calls_in"          tzstats:"notable"`
	NCallsOut     int                  `json:"n_calls_out"         tzstats:"notable"`
	NCallsFailed  int                  `json:"n_calls_failed"      tzstats:"notable"`
	Bigmaps       map[string]int64     `json:"bigmaps,omitempty"   tzstats:"notable"`
	Metadata      map[string]*Metadata `json:"metadata,omitempty"  tzstats:"notable"`

	columns []string `json:"-"`
}

func ParseU64(s string) (u uint64) {
	buf, _ := hex.DecodeString(s)
	u = binary.BigEndian.Uint64(buf[:8])
	return
}

func (c *Contract) Meta() *Metadata {
	m, ok := c.Metadata[c.Address.String()]
	if !ok {
		m = NewMetadata(c.Address)
		if c.Metadata == nil {
			c.Metadata = make(map[string]*Metadata)
		}
		c.Metadata[c.Address.String()] = m
	}
	return m
}

type ContractList struct {
	Rows    []*Contract
	columns []string
}

func (l ContractList) Len() int {
	return len(l.Rows)
}

func (l ContractList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *ContractList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("ContractList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		r := &Contract{
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

func (a *Contract) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return a.UnmarshalJSONBrief(data)
	}
	type Alias *Contract
	return json.Unmarshal(data, Alias(a))
}

func (c *Contract) UnmarshalJSONBrief(data []byte) error {
	cc := Contract{}
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
		case "account_id":
			cc.AccountId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "address":
			cc.Address, err = tezos.ParseAddress(f.(string))
		case "creator_id":
			cc.CreatorId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "creator":
			cc.Creator, err = tezos.ParseAddress(f.(string))
		case "first_seen":
			cc.FirstSeen, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "last_seen":
			cc.LastSeen, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "first_seen_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				cc.FirstSeenTime = time.Unix(0, ts*1000000).UTC()
			}
		case "last_seen_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				cc.LastSeenTime = time.Unix(0, ts*1000000).UTC()
			}
		case "storage_size":
			cc.StorageSize, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "storage_paid":
			cc.StoragePaid, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "script":
			var buf []byte
			buf, err = hex.DecodeString(f.(string))
			if err == nil {
				cc.Script = &micheline.Script{}
				err = cc.Script.UnmarshalBinary(buf)
			}
		case "storage":
			var buf []byte
			buf, err = hex.DecodeString(f.(string))
			if err == nil {
				cc.Storage = &micheline.Prim{}
				err = cc.Storage.UnmarshalBinary(buf)
			}
		case "iface_hash":
			cc.InterfaceHash = f.(string)
		case "code_hash":
			cc.CodeHash = f.(string)
		case "storage_hash":
			cc.StorageHash = f.(string)
		case "call_stats":
			var buf []byte
			buf, err = hex.DecodeString(f.(string))
			if err == nil {
				cc.CallStats = make(map[string]int)
				if cc.Script != nil {
					var eps micheline.Entrypoints
					eps, err = cc.Script.Entrypoints(false)
					for _, ep := range eps {
						if len(buf) < ep.Id*4+4 {
							continue
						}
						cc.CallStats[ep.Name] = int(binary.BigEndian.Uint32(buf[ep.Id*4:]))
					}
				} else {
					for i := 0; i < len(buf); i += 4 {
						cc.CallStats[strconv.Itoa(i/4)] = int(binary.BigEndian.Uint32(buf[i:]))
					}
				}
			}
		case "features":
			cc.Features = strings.Split(f.(string), ",")
		case "interfaces":
			cc.Interfaces = strings.Split(f.(string), ",")
		}
		if err != nil {
			return err
		}
	}
	*c = cc
	return nil
}

type ContractMeta struct {
	Address string    `json:"contract"`
	Time    time.Time `json:"time"`
	Height  int64     `json:"height"`
	Block   string    `json:"block"`
}

type ContractParameters struct {
	ContractValue                 // contract
	Entrypoint    string          `json:"entrypoint"`           // contract
	L2Address     *tezos.Address  `json:"l2_address,omitempty"` // rollup
	Method        string          `json:"method,omitempty"`     // rollup
	Arguments     json.RawMessage `json:"arguments,omitempty"`  // rollup
}

type ContractScript struct {
	Script          *micheline.Script         `json:"script,omitempty"`
	StorageType     micheline.Typedef         `json:"storage_type"`
	Entrypoints     micheline.Entrypoints     `json:"entrypoints"`
	Views           micheline.Views           `json:"views,omitempty"`
	BigmapNames     map[string]int64          `json:"bigmaps,omitempty"`
	BigmapTypes     map[string]micheline.Type `json:"bigmap_types,omitempty"`
	BigmapTypesById map[int64]micheline.Type  `json:"-"`
}

func (s ContractScript) Types() (param, store micheline.Type, eps micheline.Entrypoints, bigmaps map[int64]micheline.Type) {
	param = s.Script.ParamType()
	store = s.Script.StorageType()
	eps, _ = s.Script.Entrypoints(true)
	bigmaps = s.BigmapTypesById
	return
}

type ContractValue struct {
	Value interface{}     `json:"value,omitempty"`
	Prim  *micheline.Prim `json:"prim,omitempty"`
}

func (v ContractValue) IsPrim() bool {
	if v.Value == nil {
		return false
	}
	if m, ok := v.Value.(map[string]interface{}); !ok {
		return false
	} else {
		_, ok := m["prim"]
		return ok
	}
}

func (v ContractValue) AsPrim() (micheline.Prim, bool) {
	if v.Prim.IsValid() {
		return *v.Prim, true
	}

	if v.IsPrim() {
		buf, _ := json.Marshal(v.Value)
		p := micheline.Prim{}
		err := p.UnmarshalJSON(buf)
		return p, err == nil
	}

	return micheline.InvalidPrim, false
}

func (v ContractValue) Has(path string) bool {
	return hasPath(v.Value, path)
}

func (v ContractValue) GetString(path string) (string, bool) {
	return getPathString(v.Value, path)
}

func (v ContractValue) GetInt64(path string) (int64, bool) {
	return getPathInt64(v.Value, path)
}

func (v ContractValue) GetBig(path string) (*big.Int, bool) {
	return getPathBig(v.Value, path)
}

func (v ContractValue) GetZ(path string) (tezos.Z, bool) {
	return getPathZ(v.Value, path)
}

func (v ContractValue) GetTime(path string) (time.Time, bool) {
	return getPathTime(v.Value, path)
}

func (v ContractValue) GetAddress(path string) (tezos.Address, bool) {
	return getPathAddress(v.Value, path)
}

func (v ContractValue) GetValue(path string) (interface{}, bool) {
	return getPathValue(v.Value, path)
}

func (v ContractValue) Walk(path string, fn ValueWalkerFunc) error {
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

func (v ContractValue) Unmarshal(val interface{}) error {
	buf, _ := json.Marshal(v.Value)
	return json.Unmarshal(buf, val)
}

type ContractParams struct {
	Params
}

func NewContractParams() ContractParams {
	return ContractParams{NewParams()}
}

func (p ContractParams) WithLimit(v uint) ContractParams {
	p.Query.Set("limit", strconv.Itoa(int(v)))
	return p
}

func (p ContractParams) WithOffset(v uint) ContractParams {
	p.Query.Set("offset", strconv.Itoa(int(v)))
	return p
}

func (p ContractParams) WithCursor(v uint64) ContractParams {
	p.Query.Set("cursor", strconv.FormatUint(v, 10))
	return p
}

func (p ContractParams) WithOrder(v OrderType) ContractParams {
	p.Query.Set("order", string(v))
	return p
}

func (p ContractParams) WithBlock(v string) ContractParams {
	p.Query.Set("block", v)
	return p
}

func (p ContractParams) WithSince(v string) ContractParams {
	p.Query.Set("since", v)
	return p
}

func (p ContractParams) WithUnpack() ContractParams {
	p.Query.Set("unpack", "1")
	return p
}

func (p ContractParams) WithPrim() ContractParams {
	p.Query.Set("prim", "1")
	return p
}

func (p ContractParams) WithMeta() ContractParams {
	p.Query.Set("meta", "1")
	return p
}

func (p ContractParams) WithMerge() ContractParams {
	p.Query.Set("merge", "1")
	return p
}

func (p ContractParams) WithStorage() ContractParams {
	p.Query.Set("storage", "1")
	return p
}

type ContractQuery struct {
	tableQuery
}

func (c *Client) NewContractQuery() ContractQuery {
	tinfo, err := GetTypeInfo(&Contract{})
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.base.Copy(),
		Table:   "contract",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Order:   OrderAsc,
		Columns: tinfo.FilteredAliases("notable"),
		Filter:  make(FilterList, 0),
	}
	return ContractQuery{q}
}

func (q ContractQuery) Run(ctx context.Context) (*ContractList, error) {
	result := &ContractList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryContracts(ctx context.Context, filter FilterList, cols []string) (*ContractList, error) {
	q := c.NewContractQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}

func (c *Client) GetContract(ctx context.Context, addr tezos.Address, params ContractParams) (*Contract, error) {
	cc := &Contract{}
	u := params.AppendQuery(fmt.Sprintf("/explorer/contract/%s", addr))
	if err := c.get(ctx, u, nil, cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func (c *Client) GetContractScript(ctx context.Context, addr tezos.Address, params ContractParams) (*ContractScript, error) {
	cc := &ContractScript{}
	u := params.AppendQuery(fmt.Sprintf("/explorer/contract/%s/script", addr))
	if err := c.get(ctx, u, nil, cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func (c *Client) GetContractStorage(ctx context.Context, addr tezos.Address, params ContractParams) (*ContractValue, error) {
	cc := &ContractValue{}
	u := params.AppendQuery(fmt.Sprintf("/explorer/contract/%s/storage", addr))
	if err := c.get(ctx, u, nil, cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func (c *Client) ListContractCalls(ctx context.Context, addr tezos.Address, params ContractParams) ([]*Op, error) {
	calls := make([]*Op, 0)
	u := params.AppendQuery(fmt.Sprintf("/explorer/contract/%s/calls", addr))
	if err := c.get(ctx, u, nil, &calls); err != nil {
		return nil, err
	}
	return calls, nil
}

func (c *Client) loadCachedContractScript(ctx context.Context, addr tezos.Address) (*ContractScript, error) {
	if c.cache != nil {
		if script, ok := c.cache.Get(addr.String()); ok {
			return script.(*ContractScript), nil
		}
	}
	c.log.Tracef("Loading contract %s", addr)
	script, err := c.GetContractScript(ctx, addr, NewContractParams().WithPrim())
	if err != nil {
		return nil, err
	}
	// strip code
	script.Script.Code.Code = micheline.Prim{}
	script.Script.Code.View = micheline.Prim{}
	// fill bigmap type info
	script.BigmapNames = script.Script.Bigmaps()
	script.BigmapTypes = script.Script.BigmapTypes()
	script.BigmapTypesById = make(map[int64]micheline.Type)
	for n, v := range script.BigmapTypes {
		id := script.BigmapNames[n]
		script.BigmapTypesById[id] = v
	}
	if c.cache != nil {
		c.cache.Add(addr.String(), script)
	}
	return script, nil
}

func (c *Client) AddCachedScript(addr tezos.Address, script *micheline.Script) {
	if !addr.IsValid() || script == nil || c.cache == nil {
		return
	}
	eps, _ := script.Entrypoints(true)
	views, _ := script.Views(true, false)
	s := &ContractScript{
		Script:          script,
		StorageType:     script.StorageType().Typedef(""),
		Entrypoints:     eps,
		Views:           views,
		BigmapNames:     script.Bigmaps(),
		BigmapTypes:     script.BigmapTypes(),
		BigmapTypesById: make(map[int64]micheline.Type),
	}
	for n, v := range s.BigmapTypes {
		id := s.BigmapNames[n]
		s.BigmapTypesById[id] = v
	}
	c.cache.Add(addr.String(), s)
}
