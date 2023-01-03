// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"fmt"
)

// Indexer operation and event type
type OpType byte

// enums are allocated in chronological order with most often used ops first
const (
	OpTypeBake                 OpType = iota // 0
	OpTypeEndorsement                        // 1
	OpTypeTransaction                        // 2
	OpTypeReveal                             // 3
	OpTypeDelegation                         // 4
	OpTypeOrigination                        // 5
	OpTypeNonceRevelation                    // 6
	OpTypeActivation                         // 7
	OpTypeBallot                             // 8
	OpTypeProposal                           // 9
	OpTypeDoubleBaking                       // 10
	OpTypeDoubleEndorsement                  // 11
	OpTypeUnfreeze                           // 12 implicit event
	OpTypeInvoice                            // 13 implicit event
	OpTypeAirdrop                            // 14 implicit event
	OpTypeSeedSlash                          // 15 implicit event
	OpTypeMigration                          // 16 implicit event
	OpTypeSubsidy                            // 17 v010 liquidity baking
	OpTypeRegisterConstant                   // 18 v011
	OpTypePreendorsement                     // 19 v012
	OpTypeDoublePreendorsement               // 20 v012
	OpTypeDepositsLimit                      // 21 v012
	OpTypeDeposit                            // 22 v012 implicit event (baker deposit)
	OpTypeBonus                              // 23 v012 implicit event (baker extra bonus)
	OpTypeReward                             // 24 v012 implicit event (endorsement reward pay/burn)
	OpTypeRollupOrigination                  // 25 v013 rollup
	OpTypeRollupTransaction                  // 26 v013 rollup
	OpTypeVdfRevelation                      // 27 v014
	OpTypeIncreasePaidStorage                // 28 v014
	OpTypeDrainDelegate                      // 29 v015
	OpTypeUpdateConsensusKey                 // 30 v015
	OpTypeBatch                = 254         // API output only
	OpTypeInvalid              = 255
)

var (
	opTypeStrings = map[OpType]string{
		OpTypeBake:                 "bake",
		OpTypeEndorsement:          "endorsement",
		OpTypeTransaction:          "transaction",
		OpTypeReveal:               "reveal",
		OpTypeDelegation:           "delegation",
		OpTypeOrigination:          "origination",
		OpTypeNonceRevelation:      "nonce_revelation",
		OpTypeActivation:           "activation",
		OpTypeBallot:               "ballot",
		OpTypeProposal:             "proposal",
		OpTypeDoubleBaking:         "double_baking",
		OpTypeDoubleEndorsement:    "double_endorsement",
		OpTypeUnfreeze:             "unfreeze",
		OpTypeInvoice:              "invoice",
		OpTypeAirdrop:              "airdrop",
		OpTypeSeedSlash:            "seed_slash",
		OpTypeMigration:            "migration",
		OpTypeSubsidy:              "subsidy",
		OpTypeRegisterConstant:     "register_constant",
		OpTypePreendorsement:       "preendorsement",
		OpTypeDoublePreendorsement: "double_preendorsement",
		OpTypeDepositsLimit:        "deposits_limit",
		OpTypeDeposit:              "deposit",
		OpTypeReward:               "reward",
		OpTypeBonus:                "bonus",
		OpTypeBatch:                "batch",
		OpTypeRollupOrigination:    "rollup_origination",
		OpTypeRollupTransaction:    "rollup_transaction",
		OpTypeVdfRevelation:        "vdf_revelation",
		OpTypeIncreasePaidStorage:  "increase_paid_storage",
		OpTypeDrainDelegate:        "drain_delegate",
		OpTypeUpdateConsensusKey:   "update_consensus_key",
		OpTypeInvalid:              "",
	}
	opTypeReverseStrings = make(map[string]OpType)
)

func init() {
	for n, v := range opTypeStrings {
		opTypeReverseStrings[v] = n
	}
}

func (t OpType) IsValid() bool {
	return t != OpTypeInvalid
}

func (t *OpType) UnmarshalText(data []byte) error {
	v := ParseOpType(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid operation type '%s'", string(data))
	}
	*t = v
	return nil
}

func (t *OpType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func ParseOpType(s string) OpType {
	t, ok := opTypeReverseStrings[s]
	if !ok {
		t = OpTypeInvalid
	}
	return t
}

func (t OpType) String() string {
	return opTypeStrings[t]
}
