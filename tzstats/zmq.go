// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzgo/tezos"
)

type ZmqMessage struct {
	topic  string
	body   []byte
	fields []string
}

func NewZmqMessage(topic, body []byte) *ZmqMessage {
	return &ZmqMessage{string(topic), body, nil}
}

func (m *ZmqMessage) GetField(name string) (string, bool) {
	return getTableColumn(m.body, zmqFields(m.topic), name)
}

func (m *ZmqMessage) DecodeOpHash() (tezos.OpHash, error) {
	return tezos.ParseOpHash(string(m.body))
}

func (m *ZmqMessage) DecodeBlockHash() (tezos.BlockHash, error) {
	return tezos.ParseBlockHash(string(m.body))
}

func (m *ZmqMessage) DecodeOp() (*Op, error) {
	o := new(Op).WithColumns(ZmqRawOpColumns...)
	if err := json.Unmarshal(m.body, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (m *ZmqMessage) DecodeOpWithScript(ctx context.Context, c *Client) (*Op, error) {
	o := new(Op).WithColumns(ZmqRawOpColumns...)

	// we may need contract scripts
	if is, ok := m.GetField("is_contract"); ok && is == "1" {
		recv, ok := m.GetField("receiver")
		if ok && recv != "" && recv != "null" {
			addr, err := tezos.ParseAddress(recv)
			if err != nil {
				return nil, fmt.Errorf("decode: invalid receiver address %s: %v, %#v", recv, err, string(m.body))
			}
			// load contract type info (required for decoding storage/param data)
			script, err := c.loadCachedContractScript(ctx, addr)
			if err != nil {
				return nil, err
			}
			o = o.WithScript(script)
		}
	}
	if err := json.Unmarshal(m.body, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (m *ZmqMessage) DecodeBlock() (*Block, error) {
	b := new(Block).WithColumns(ZmqRawBlockColumns...)
	if err := json.Unmarshal(m.body, b); err != nil {
		return nil, err
	}
	return b, nil
}

func (m *ZmqMessage) DecodeStatus() (*Status, error) {
	s := new(Status).WithColumns(ZmqStatusColumns...)
	if err := json.Unmarshal(m.body, s); err != nil {
		return nil, err
	}
	return s, nil
}

func zmqFields(topic string) []string {
	switch topic {
	case "raw_block", "raw_block/rollback":
		return ZmqRawBlockColumns
	case "raw_op", "raw_op/rollback":
		return ZmqRawOpColumns
	case "status":
		return ZmqStatusColumns
	default:
		return nil
	}
}

var ZmqRawBlockColumns = []string{
	"row_id",
	"hash",
	"predecessor",
	"height",
	"cycle",
	"time",
	"solvetime",
	"version",
	"round",
	"nonce",
	"voting_period_kind",
	"baker",
	"proposer",
	"n_ops_applied",
	"n_ops_failed",
	"n_calls",
	"n_rollup_calls",
	"n_events",
	"volume",
	"fee",
	"reward",
	"deposit",
	"activated_supply",
	"burned_supply",
	"minted_supply",
	"n_accounts",
	"n_new_accounts",
	"n_new_contracts",
	"n_cleared_accounts",
	"n_funded_accounts",
	"gas_limit",
	"gas_used",
	"storage_paid",
	"lb_esc_vote",
	"lb_esc_ema",
	"protocol",
}

var ZmqRawOpColumns = []string{
	"id",
	"type",
	"hash",
	"block",
	"height",
	"cycle",
	"time",
	"op_n",
	"op_p",
	"op_c",
	"op_i",
	"status",
	"is_success",
	"is_contract",
	"is_internal",
	"is_event",
	"is_rollup",
	"counter",
	"gas_limit",
	"gas_used",
	"storage_limit",
	"storage_paid",
	"volume",
	"fee",
	"reward",
	"deposit",
	"burned",
	"sender_id",
	"sender",
	"receiver_id",
	"receiver",
	"creator_id",
	"creator",
	"baker_id",
	"baker",
	"data",
	"parameters",
	"storage",
	"big_map_diff",
	"errors",
	"entrypoint",
}

var ZmqStatusColumns = []string{
	"status",
	"blocks",
	"finalized",
	"indexed",
	"progress",
}
