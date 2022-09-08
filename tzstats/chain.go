// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type Chain struct {
	RowId                uint64    `json:"row_id"`
	Height               int64     `json:"height"`
	Cycle                int64     `json:"cycle"`
	Timestamp            time.Time `json:"time"`
	TotalAccounts        int64     `json:"total_accounts"`
	TotalContracts       int64     `json:"total_contracts"`
	TotalRollups         int64     `json:"total_rollups"`
	TotalOps             int64     `json:"total_ops"`
	TotalOpsFailed       int64     `json:"total_ops_failed"`
	TotalContractOps     int64     `json:"total_contract_ops"`
	TotalContractCalls   int64     `json:"total_contract_calls"`
	TotalRollupCalls     int64     `json:"total_rollup_calls"`
	TotalActivations     int64     `json:"total_activations"`
	TotalNonces          int64     `json:"total_nonce_revelations"`
	TotalEndorsements    int64     `json:"total_endorsements"`
	TotalPreendorsements int64     `json:"total_preendorsements"`
	TotalDoubleBake      int64     `json:"total_double_bakings"`
	TotalDoubleEndorse   int64     `json:"total_double_endorsements"`
	TotalDelegations     int64     `json:"total_delegations"`
	TotalReveals         int64     `json:"total_reveals"`
	TotalOriginations    int64     `json:"total_originations"`
	TotalTransactions    int64     `json:"total_transactions"`
	TotalProposals       int64     `json:"total_proposals"`
	TotalBallots         int64     `json:"total_ballots"`
	TotalConstants       int64     `json:"total_constants"`
	TotalSetLimits       int64     `json:"total_set_limits"`
	TotalStorageBytes    int64     `json:"total_storage_bytes"`
	FundedAccounts       int64     `json:"funded_accounts"`
	DustAccounts         int64     `json:"dust_accounts"`
	GhostAccounts        int64     `json:"ghost_accounts"`
	UnclaimedAccounts    int64     `json:"unclaimed_accounts"`
	TotalDelegators      int64     `json:"total_delegators"`
	ActiveDelegators     int64     `json:"active_delegators"`
	InactiveDelegators   int64     `json:"inactive_delegators"`
	DustDelegators       int64     `json:"dust_delegators"`
	TotalBakers          int64     `json:"total_bakers"`
	ActiveBakers         int64     `json:"active_bakers"`
	InactiveBakers       int64     `json:"inactive_bakers"`
	ZeroBakers           int64     `json:"zero_bakers"`
	SelfBakers           int64     `json:"self_bakers"`
	SingleBakers         int64     `json:"single_bakers"`
	MultiBakers          int64     `json:"multi_bakers"`
	Rolls                int64     `json:"rolls"`
	RollOwners           int64     `json:"roll_owners"`

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
	if len(data) == 0 || bytes.Equal(data, null) {
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
	if len(data) == 0 || bytes.Equal(data, null) {
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
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				cc.Timestamp = time.Unix(0, ts*1000000).UTC()
			}
		case "total_accounts":
			cc.TotalAccounts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_contracts":
			cc.TotalContracts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_rollups":
			cc.TotalRollups, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_ops":
			cc.TotalOps, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_ops_failed":
			cc.TotalOpsFailed, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_contract_ops":
			cc.TotalContractOps, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_contract_calls":
			cc.TotalContractCalls, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_rollup_calls":
			cc.TotalRollupCalls, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_activations":
			cc.TotalActivations, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_nonce_revelations":
			cc.TotalNonces, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_endorsements":
			cc.TotalEndorsements, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_double_bakings":
			cc.TotalDoubleBake, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_double_endorsements":
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
		case "total_set_limits":
			cc.TotalSetLimits, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "total_storage_bytes":
			cc.TotalStorageBytes, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "funded_accounts":
			cc.FundedAccounts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "dust_accounts":
			cc.DustAccounts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "ghost_accounts":
			cc.GhostAccounts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
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
		case "total_bakers":
			cc.TotalBakers, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "active_bakers":
			cc.ActiveBakers, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "inactive_bakers":
			cc.InactiveBakers, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "zero_bakers":
			cc.ZeroBakers, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "self_bakers":
			cc.SelfBakers, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "single_bakers":
			cc.SingleBakers, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "multi_bakers":
			cc.MultiBakers, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
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
	tinfo, err := GetTypeInfo(&Chain{})
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
