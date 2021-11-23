// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

type Chain struct {
	RowId              uint64 `json:"row_id"`
	Height             int64  `json:"height"`
	Cycle              int64  `json:"cycle"`
	Timestamp          int64  `json:"time"`
	TotalAccounts      int64  `json:"total_accounts"`
	TotalContracts     int64  `json:"total_contracts"`
	TotalOps           int64  `json:"total_ops"`
	TotalContractOps   int64  `json:"total_contract_ops"`
	TotalContractCalls int64  `json:"total_contract_calls"`
	TotalActivations   int64  `json:"total_activations"`
	TotalSeedNonces    int64  `json:"total_seed_nonce_revelations"`
	TotalEndorsements  int64  `json:"total_endorsements"`
	TotalDoubleBake    int64  `json:"total_double_baking_evidences"`
	TotalDoubleEndorse int64  `json:"total_double_endorsement_evidences"`
	TotalDelegations   int64  `json:"total_delegations"`
	TotalReveals       int64  `json:"total_reveals"`
	TotalOriginations  int64  `json:"total_originations"`
	TotalTransactions  int64  `json:"total_transactions"`
	TotalProposals     int64  `json:"total_proposals"`
	TotalBallots       int64  `json:"total_ballots"`
	TotalConstants     int64  `json:"total_constants"`
	TotalStorageBytes  int64  `json:"total_storage_bytes"`
	TotalPaidBytes     int64  `json:"total_paid_bytes"`
	FundedAccounts     int64  `json:"funded_accounts"`
	DustAccounts       int64  `json:"dust_accounts"`
	UnclaimedAccounts  int64  `json:"unclaimed_accounts"`
	TotalDelegators    int64  `json:"total_delegators"`
	ActiveDelegators   int64  `json:"active_delegators"`
	InactiveDelegators int64  `json:"inactive_delegators"`
	DustDelegators     int64  `json:"dust_delegators"`
	TotalDelegates     int64  `json:"total_delegates"`
	ActiveDelegates    int64  `json:"active_delegates"`
	InactiveDelegates  int64  `json:"inactive_delegates"`
	ZeroDelegates      int64  `json:"zero_delegates"`
	SelfDelegates      int64  `json:"self_delegates"`
	SingleDelegates    int64  `json:"single_delegates"`
	MultiDelegates     int64  `json:"multi_delegates"`
	Rolls              int64  `json:"rolls"`
	RollOwners         int64  `json:"roll_owners"`

	columns []string `json:"-"`
}

type ChainList struct {
	Rows    []*Chain
	columns []string
}

func (l ChainList) Len() int {
	return len(l.Rows)
}

func (l ChainList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *ChainList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Compare(data, []byte("null")) == 0 {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("ChainList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		r := &Chain{
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

func (a *Chain) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Compare(data, []byte("null")) == 0 {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return a.UnmarshalJSONBrief(data)
	}
	type Alias *Chain
	return json.Unmarshal(data, Alias(a))
}

func (c *Chain) UnmarshalJSONBrief(data []byte) error {
	cc := Chain{}
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
		case "height":
			cc.Height, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "cycle":
			cc.Cycle, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "time":
			cc.Timestamp, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_accounts":
			cc.TotalAccounts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_contracts":
			cc.TotalContracts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_ops":
			cc.TotalOps, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_contract_ops":
			cc.TotalContractOps, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_contract_calls":
			cc.TotalContractCalls, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_activations":
			cc.TotalActivations, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_seed_nonce_revelations":
			cc.TotalSeedNonces, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_endorsements":
			cc.TotalEndorsements, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_double_baking_evidences":
			cc.TotalDoubleBake, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_double_endorsement_evidences":
			cc.TotalDoubleEndorse, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_delegations":
			cc.TotalDelegations, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_reveals":
			cc.TotalReveals, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_originations":
			cc.TotalOriginations, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_transactions":
			cc.TotalTransactions, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_proposals":
			cc.TotalProposals, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_ballots":
			cc.TotalBallots, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_constants":
			cc.TotalConstants, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_storage_bytes":
			cc.TotalStorageBytes, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_paid_bytes":
			cc.TotalPaidBytes, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "funded_accounts":
			cc.FundedAccounts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "dust_accounts":
			cc.DustAccounts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "unclaimed_accounts":
			cc.UnclaimedAccounts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_delegators":
			cc.TotalDelegators, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "active_delegators":
			cc.ActiveDelegators, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "inactive_delegators":
			cc.InactiveDelegators, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "dust_delegators":
			cc.DustDelegators, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_delegates":
			cc.TotalDelegates, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "active_delegates":
			cc.ActiveDelegates, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "inactive_delegates":
			cc.InactiveDelegates, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "zero_delegates":
			cc.ZeroDelegates, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "self_delegates":
			cc.SelfDelegates, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "single_delegates":
			cc.SingleDelegates, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "multi_delegates":
			cc.MultiDelegates, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "rolls":
			cc.Rolls, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "roll_owners":
			cc.RollOwners, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		}
		if err != nil {
			return err
		}
	}
	*c = cc
	return nil
}

type ChainQuery struct {
	tableQuery
}

func (c *Client) NewChainQuery() ChainQuery {
	tinfo, err := GetTypeInfo(&Chain{}, "")
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.params.Copy(),
		Table:   "chain",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Order:   OrderAsc,
		Columns: tinfo.Aliases(),
		Filter:  make(FilterList, 0),
	}
	return ChainQuery{q}
}

func (q ChainQuery) Run(ctx context.Context) (*ChainList, error) {
	result := &ChainList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryChains(ctx context.Context, filter FilterList, cols []string) (*ChainList, error) {
	q := c.NewChainQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}
