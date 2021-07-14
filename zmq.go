// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"bytes"
	"encoding/json"

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

// Split JSON array into fields, takes care of strings with enclosed comma
func (m *ZmqMessage) unpack() {
	if len(m.fields) > 0 {
		return
	}
	fields := make([]json.RawMessage, 0)
	_ = json.Unmarshal(m.body, &fields)
	for _, v := range fields {
		m.fields = append(m.fields, string(bytes.Trim(v, "\"")))
	}
}

func (m *ZmqMessage) GetField(name string) (string, bool) {
	if idx := zmqFieldIndex(m.topic, name); idx < 0 {
		return "", false
	} else {
		m.unpack()
		return m.fields[idx], true
	}
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

func (m *ZmqMessage) DecodeOpWithScript(s *ContractScript) (*Op, error) {
	o := new(Op).WithColumns(ZmqRawOpColumns...).WithScript(s)
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

func zmqFields(topic string) []string {
	switch topic {
	case "raw_block", "raw_block/rollback":
		return ZmqRawBlockColumns
	case "raw_op", "raw_op/rollback":
		return ZmqRawOpColumns
	default:
		return nil
	}
}

func zmqFieldIndex(topic, name string) int {
	for i, v := range zmqFields(topic) {
		if v != name {
			continue
		}
		return i
	}
	return -1
}

var ZmqRawBlockColumns = []string{
	"row_id",
	"parent_id",
	"hash",
	"is_orphan",
	"height",
	"cycle",
	"is_cycle_snapshot",
	"time",
	"solvetime",
	"version",
	"validation_pass",
	"fitness",
	"priority",
	"nonce",
	"voting_period_kind",
	"baker_id",
	"endorsed_slots",
	"n_endorsed_slots",
	"n_ops",
	"n_ops_failed",
	"n_ops_contract",
	"n_tx",
	"n_activation",
	"n_seed_nonce_revelation",
	"n_double_baking_evidence",
	"n_double_endorsement_evidence",
	"n_endorsement",
	"n_delegation",
	"n_reveal",
	"n_origination",
	"n_proposal",
	"n_ballot",
	"volume",
	"fee",
	"reward",
	"deposit",
	"unfrozen_fees",
	"unfrozen_rewards",
	"unfrozen_deposits",
	"activated_supply",
	"burned_supply",
	"n_accounts",
	"n_new_accounts",
	"n_new_implicit",
	"n_new_managed",
	"n_new_contracts",
	"n_cleared_accounts",
	"n_funded_accounts",
	"gas_limit",
	"gas_used",
	"gas_price",
	"storage_size",
	"days_destroyed",
	"n_ops_implicit",
	"pct_account_reuse",
	"baker",
	"predecessor",
}

var ZmqRawOpColumns = []string{
	"row_id",
	"time",
	"height",
	"cycle",
	"hash",
	"counter",
	"op_n",
	"op_c",
	"op_i",
	"op_l",
	"op_p",
	"type",
	"status",
	"is_success",
	"is_contract",
	"gas_limit",
	"gas_used",
	"gas_price",
	"storage_limit",
	"storage_size",
	"storage_paid",
	"volume",
	"fee",
	"reward",
	"deposit",
	"burned",
	"sender_id",
	"receiver_id",
	"creator_id",
	"delegate_id",
	"is_internal",
	"has_data",
	"data",
	"parameters",
	"storage",
	"big_map_diff",
	"errors",
	"days_destroyed",
	"branch_id",
	"branch_height",
	"branch_depth",
	"is_implicit",
	"entrypoint_id",
	"is_orphan",
	"sender",
	"receiver",
	"creator",
	"delegate",
	"is_batch",
	"is_sapling",
	"block",
}
