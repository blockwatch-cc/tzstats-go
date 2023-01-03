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

//nolint:staticcheck
type Account struct {
	RowId              uint64              `json:"row_id"`
	Address            tezos.Address       `json:"address"`
	AddressType        tezos.AddressType   `json:"address_type"`
	Pubkey             tezos.Key           `json:"pubkey"`
	Counter            int64               `json:"counter"`
	BakerId            uint64              `json:"baker_id,omitempty"`
	Baker              *tezos.Address      `json:"baker,omitempty"`
	CreatorId          uint64              `json:"creator_id,omitempty"`
	Creator            *tezos.Address      `json:"creator,omitempty"`
	FirstIn            int64               `json:"first_in"`
	FirstOut           int64               `json:"first_out"`
	FirstSeen          int64               `json:"first_seen"`
	LastIn             int64               `json:"last_in"`
	LastOut            int64               `json:"last_out"`
	LastSeen           int64               `json:"last_seen"`
	FirstSeenTime      time.Time           `json:"first_seen_time"`
	LastSeenTime       time.Time           `json:"last_seen_time"`
	FirstInTime        time.Time           `json:"first_in_time"`
	LastInTime         time.Time           `json:"last_in_time"`
	FirstOutTime       time.Time           `json:"first_out_time"`
	LastOutTime        time.Time           `json:"last_out_time"`
	DelegatedSince     int64               `json:"delegated_since"`
	DelegatedSinceTime time.Time           `json:"delegated_since_time"`
	TotalReceived      float64             `json:"total_received"`
	TotalSent          float64             `json:"total_sent"`
	TotalBurned        float64             `json:"total_burned"`
	TotalFeesPaid      float64             `json:"total_fees_paid"`
	TotalFeesUsed      float64             `json:"total_fees_used"`
	UnclaimedBalance   float64             `json:"unclaimed_balance,omitempty"`
	SpendableBalance   float64             `json:"spendable_balance"`
	FrozenBond         float64             `json:"frozen_bond"`
	LostBond           float64             `json:"lost_bond"`
	IsFunded           bool                `json:"is_funded"`
	IsActivated        bool                `json:"is_activated"`
	IsDelegated        bool                `json:"is_delegated"`
	IsRevealed         bool                `json:"is_revealed"`
	IsBaker            bool                `json:"is_baker"`
	IsContract         bool                `json:"is_contract"`
	NTxSuccess         int                 `json:"n_tx_success"`
	NTxFailed          int                 `json:"n_tx_failed"`
	NTxOut             int                 `json:"n_tx_out"`
	NTxIn              int                 `json:"n_tx_in"`
	LifetimeRewards    float64             `json:"lifetime_rewards,omitempty"`
	PendingRewards     float64             `json:"pending_rewards,omitempty"`
	Metadata           map[string]Metadata `json:"metadata,omitempty" tzpro:"notable"`
	columns            []string            `json:"-"`
}

type AccountList struct {
	Rows    []*Account
	columns []string
}

func (l AccountList) Len() int {
	return len(l.Rows)
}

func (l AccountList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *AccountList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("AccountList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		r := &Account{
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

func (a *Account) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return a.UnmarshalJSONBrief(data)
	}
	type Alias *Account
	return json.Unmarshal(data, Alias(a))
}

func (a *Account) UnmarshalJSONBrief(data []byte) error {
	acc := Account{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	unpacked := make([]interface{}, 0)
	err := dec.Decode(&unpacked)
	if err != nil {
		return err
	}
	for i, v := range a.columns {
		f := unpacked[i]
		if f == nil {
			continue
		}
		switch v {
		case "row_id":
			acc.RowId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "address":
			acc.Address, err = tezos.ParseAddress(f.(string))
		case "address_type":
			acc.AddressType = tezos.ParseAddressType(f.(string))
		case "pubkey":
			acc.Pubkey, err = tezos.ParseKey(f.(string))
		case "counter":
			acc.Counter, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "baker_id":
			acc.BakerId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "baker":
			var a tezos.Address
			a, err = tezos.ParseAddress(f.(string))
			if err == nil {
				acc.Baker = &a
			}
		case "creator_id":
			acc.CreatorId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "creator":
			var a tezos.Address
			a, err = tezos.ParseAddress(f.(string))
			if err == nil {
				acc.Creator = &a
			}
		case "first_in":
			acc.FirstIn, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "first_out":
			acc.FirstOut, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "first_seen":
			acc.FirstSeen, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "last_in":
			acc.LastIn, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "last_out":
			acc.LastOut, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "last_seen":
			acc.LastSeen, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "delegated_since":
			acc.DelegatedSince, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_received":
			acc.TotalReceived, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "total_sent":
			acc.TotalSent, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "total_burned":
			acc.TotalBurned, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "total_fees_paid":
			acc.TotalFeesPaid, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "total_fees_used":
			acc.TotalFeesUsed, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "unclaimed_balance":
			acc.UnclaimedBalance, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "spendable_balance":
			acc.SpendableBalance, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "frozen_bond":
			acc.FrozenBond, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "lost_bond":
			acc.LostBond, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "is_funded":
			acc.IsFunded, err = strconv.ParseBool(f.(json.Number).String())
		case "is_activated":
			acc.IsActivated, err = strconv.ParseBool(f.(json.Number).String())
		case "is_delegated":
			acc.IsDelegated, err = strconv.ParseBool(f.(json.Number).String())
		case "is_revealed":
			acc.IsRevealed, err = strconv.ParseBool(f.(json.Number).String())
		case "is_baker":
			acc.IsBaker, err = strconv.ParseBool(f.(json.Number).String())
		case "is_contract":
			acc.IsContract, err = strconv.ParseBool(f.(json.Number).String())
		case "n_tx_success":
			acc.NTxSuccess, err = strconv.Atoi(f.(json.Number).String())
		case "n_tx_failed":
			acc.NTxFailed, err = strconv.Atoi(f.(json.Number).String())
		case "n_tx_out":
			acc.NTxOut, err = strconv.Atoi(f.(json.Number).String())
		case "n_tx_in":
			acc.NTxIn, err = strconv.Atoi(f.(json.Number).String())
		case "first_seen_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				acc.FirstSeenTime = time.Unix(0, ts*1000000).UTC()
			}
		case "last_seen_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				acc.LastSeenTime = time.Unix(0, ts*1000000).UTC()
			}
		case "first_in_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				acc.FirstInTime = time.Unix(0, ts*1000000).UTC()
			}
		case "last_in_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				acc.LastInTime = time.Unix(0, ts*1000000).UTC()
			}
		case "first_out_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				acc.FirstOutTime = time.Unix(0, ts*1000000).UTC()
			}
		case "last_out_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				acc.LastOutTime = time.Unix(0, ts*1000000).UTC()
			}
		case "delegated_since_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				acc.DelegatedSinceTime = time.Unix(0, ts*1000000).UTC()
			}
		}
		if err != nil {
			return err
		}
	}
	*a = acc
	return nil
}

type AccountParams struct {
	Params
}

func NewAccountParams() AccountParams {
	return AccountParams{NewParams()}
}

func (p AccountParams) WithLimit(v uint) AccountParams {
	p.Query.Set("limit", strconv.Itoa(int(v)))
	return p
}

func (p AccountParams) WithOffset(v uint) AccountParams {
	p.Query.Set("offset", strconv.Itoa(int(v)))
	return p
}

func (p AccountParams) WithCursor(v uint64) AccountParams {
	p.Query.Set("cursor", strconv.FormatUint(v, 10))
	return p
}

func (p AccountParams) WithOrder(v OrderType) AccountParams {
	p.Query.Set("order", string(v))
	return p
}

func (p AccountParams) WithMeta() AccountParams {
	p.Query.Set("meta", "1")
	return p
}

type AccountQuery struct {
	tableQuery
}

func (c *Client) NewAccountQuery() AccountQuery {
	tinfo, err := GetTypeInfo(&Account{})
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.base.Copy(),
		Table:   "account",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Order:   OrderAsc,
		Columns: tinfo.FilteredAliases("notable"),
		Filter:  make(FilterList, 0),
	}
	return AccountQuery{q}
}

func (q AccountQuery) Run(ctx context.Context) (*AccountList, error) {
	result := &AccountList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryAccounts(ctx context.Context, filter FilterList, cols []string) (*AccountList, error) {
	q := c.NewAccountQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}

func (c *Client) GetAccount(ctx context.Context, addr tezos.Address, params AccountParams) (*Account, error) {
	a := &Account{}
	u := params.AppendQuery(fmt.Sprintf("/explorer/account/%s", addr))
	if err := c.get(ctx, u, nil, a); err != nil {
		return nil, err
	}
	return a, nil
}

func (c *Client) GetAccountContracts(ctx context.Context, addr tezos.Address, params AccountParams) ([]*Account, error) {
	cc := make([]*Account, 0)
	u := params.AppendQuery(fmt.Sprintf("/explorer/account/%s/contracts", addr))
	if err := c.get(ctx, u, nil, &cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func (c *Client) GetAccountOps(ctx context.Context, addr tezos.Address, params OpParams) ([]*Op, error) {
	ops := make([]*Op, 0)
	u := params.AppendQuery(fmt.Sprintf("/explorer/account/%s/operations", addr))
	if err := c.get(ctx, u, nil, &ops); err != nil {
		return nil, err
	}
	return ops, nil
}
