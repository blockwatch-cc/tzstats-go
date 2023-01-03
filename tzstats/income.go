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

type Income struct {
	RowId                  uint64        `json:"row_id"`
	Cycle                  int64         `json:"cycle"`
	Address                tezos.Address `json:"address"`
	AccountId              uint64        `json:"account_id"`
	Rolls                  int64         `json:"rolls"`
	Balance                float64       `json:"balance"`
	Delegated              float64       `json:"delegated"`
	ActiveStake            float64       `json:"active_stake"`
	NDelegations           int64         `json:"n_delegations"`
	NBakingRights          int64         `json:"n_baking_rights"`
	NEndorsingRights       int64         `json:"n_endorsing_rights"`
	Luck                   float64       `json:"luck"`
	LuckPct                float64       `json:"luck_percent"`
	ContributionPct        float64       `json:"contribution_percent"`
	PerformancePct         float64       `json:"performance_percent"`
	NBlocksBaked           int64         `json:"n_blocks_baked"`
	NBlocksProposed        int64         `json:"n_blocks_proposed"`
	NBlocksNotBaked        int64         `json:"n_blocks_not_baked"`
	NBlocksEndorsed        int64         `json:"n_blocks_endorsed"`
	NBlocksNotEndorsed     int64         `json:"n_blocks_not_endorsed"`
	NSlotsEndorsed         int64         `json:"n_slots_endorsed"`
	NSeedsRevealed         int64         `json:"n_seeds_revealed"`
	ExpectedIncome         float64       `json:"expected_income"`
	TotalIncome            float64       `json:"total_income"`
	TotalDeposits          float64       `json:"total_deposits"`
	BakingIncome           float64       `json:"baking_income"`
	EndorsingIncome        float64       `json:"endorsing_income"`
	AccusationIncome       float64       `json:"accusation_income"`
	SeedIncome             float64       `json:"seed_income"`
	FeesIncome             float64       `json:"fees_income"`
	TotalLoss              float64       `json:"total_loss"`
	AccusationLoss         float64       `json:"accusation_loss"`
	SeedLoss               float64       `json:"seed_loss"`
	EndorsingLoss          float64       `json:"endorsing_loss"`
	LostAccusationFees     float64       `json:"lost_accusation_fees"`
	LostAccusationRewards  float64       `json:"lost_accusation_rewards"`
	LostAccusationDeposits float64       `json:"lost_accusation_deposits"`
	LostSeedFees           float64       `json:"lost_seed_fees"`
	LostSeedRewards        float64       `json:"lost_seed_rewards"`
	StartTime              time.Time     `json:"start_time"`
	EndTime                time.Time     `json:"end_time"`
	columns                []string      `json:"-"`
}

type IncomeList struct {
	Rows    []*Income
	columns []string
}

func (l IncomeList) Len() int {
	return len(l.Rows)
}

func (l IncomeList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *IncomeList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("IncomeList: expected JSON array")
	}
	array := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &array); err != nil {
		return err
	}
	for _, v := range array {
		r := &Income{
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

func (s *Income) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if len(data) == 2 {
		return nil
	}
	if data[0] == '[' {
		return s.UnmarshalJSONBrief(data)
	}
	type Alias *Income
	return json.Unmarshal(data, Alias(s))
}

func (s *Income) UnmarshalJSONBrief(data []byte) error {
	income := Income{}
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
			income.RowId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "cycle":
			income.Cycle, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "account_id":
			income.AccountId, err = strconv.ParseUint(f.(json.Number).String(), 10, 64)
		case "address":
			income.Address, err = tezos.ParseAddress(f.(string))
		case "rolls":
			income.Rolls, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "balance":
			income.Balance, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "delegated":
			income.Delegated, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "active_stake":
			income.ActiveStake, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "n_delegations":
			income.NDelegations, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_baking_rights":
			income.NBakingRights, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_endorsing_rights":
			income.NEndorsingRights, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "luck":
			income.Luck, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "luck_percent":
			income.LuckPct, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "contribution_percent":
			income.ContributionPct, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "performance_percent":
			income.PerformancePct, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "n_blocks_baked":
			income.NBlocksBaked, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_blocks_proposed":
			income.NBlocksProposed, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_blocks_not_baked":
			income.NBlocksNotBaked, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_blocks_endorsed":
			income.NBlocksEndorsed, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_blocks_not_endorsed":
			income.NBlocksNotEndorsed, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_slots_endorsed":
			income.NSlotsEndorsed, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "n_seeds_revealed":
			income.NSeedsRevealed, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
		case "expected_income":
			income.ExpectedIncome, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "total_income":
			income.TotalIncome, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "total_deposits":
			income.TotalDeposits, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "baking_income":
			income.BakingIncome, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "endorsing_income":
			income.EndorsingIncome, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "accusation_income":
			income.AccusationIncome, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "seed_income":
			income.SeedIncome, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "fees_income":
			income.FeesIncome, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "total_loss":
			income.TotalLoss, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "accusation_loss":
			income.AccusationLoss, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "seed_loss":
			income.SeedLoss, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "endorsing_loss":
			income.EndorsingLoss, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "lost_accusation_fees":
			income.LostAccusationFees, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "lost_accusation_rewards":
			income.LostAccusationRewards, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "lost_accusation_deposits":
			income.LostAccusationDeposits, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "lost_seed_fees":
			income.LostSeedFees, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "lost_seed_rewards":
			income.LostSeedRewards, err = strconv.ParseFloat(f.(json.Number).String(), 64)
		case "start_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				income.StartTime = time.Unix(0, ts*1000000).UTC()
			}
		case "end_time":
			var ts int64
			ts, err = strconv.ParseInt(f.(json.Number).String(), 10, 64)
			if err == nil {
				income.EndTime = time.Unix(0, ts*1000000).UTC()
			}
		}
		if err != nil {
			return err
		}
	}
	*s = income
	return nil
}

type IncomeQuery struct {
	tableQuery
}

func (c *Client) NewIncomeQuery() IncomeQuery {
	tinfo, err := GetTypeInfo(&Income{})
	if err != nil {
		panic(err)
	}
	q := tableQuery{
		client:  c,
		Params:  c.base.Copy(),
		Table:   "income",
		Format:  FormatJSON,
		Limit:   DefaultLimit,
		Columns: tinfo.Aliases(),
		Order:   OrderAsc,
		Filter:  make(FilterList, 0),
	}
	return IncomeQuery{q}
}

func (q IncomeQuery) Run(ctx context.Context) (*IncomeList, error) {
	result := &IncomeList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) QueryIncome(ctx context.Context, filter FilterList, cols []string) (*IncomeList, error) {
	q := c.NewIncomeQuery()
	if len(cols) > 0 {
		q.Columns = cols
	}
	if len(filter) > 0 {
		q.Filter = filter
	}
	return q.Run(ctx)
}
