// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

type BigmapKey struct {
	Key     MultiKey        `json:"key"`
	KeyHash tezos.ExprHash  `json:"hash"`
	Meta    *BigmapMeta     `json:"meta"`
	Prim    *micheline.Prim `json:"prim"`
}

type MultiKey struct {
	named  map[string]interface{}
	anon   []interface{}
	single string
}

func DecodeMultiKey(key micheline.Key) (MultiKey, error) {
	mk := MultiKey{}
	buf, err := json.Marshal(key)
	if err != nil {
		return mk, err
	}
	err = json.Unmarshal(buf, &mk)
	return mk, err
}

func (k MultiKey) Len() int {
	if len(k.single) > 0 {
		return 1
	}
	return len(k.named) + len(k.anon)
}

func (k MultiKey) String() string {
	switch true {
	case len(k.named) > 0:
		strs := make([]string, 0)
		for n, v := range k.named {
			strs = append(strs, fmt.Sprintf("%s=%s", n, v))
		}
		return strings.Join(strs, ",")
	case len(k.anon) > 0:
		strs := make([]string, 0)
		for _, v := range k.anon {
			strs = append(strs, ToString(v))
		}
		return strings.Join(strs, ",")
	default:
		return k.single
	}
}

func (k MultiKey) MarshalJSON() ([]byte, error) {
	switch true {
	case len(k.named) > 0:
		return json.Marshal(k.named)
	case len(k.anon) > 0:
		return json.Marshal(k.anon)
	default:
		return []byte(strconv.Quote(k.single)), nil
	}
}

func (k *MultiKey) UnmarshalJSON(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	switch buf[0] {
	case '{':
		m := make(map[string]interface{})
		if err := json.Unmarshal(buf, &m); err != nil {
			return err
		}
		k.named = m
	case '[':
		m := make([]interface{}, 0)
		if err := json.Unmarshal(buf, &m); err != nil {
			return err
		}
		k.anon = m
	case '"':
		s, _ := strconv.Unquote(string(buf))
		k.single = s
	default:
		k.single = string(buf)
	}
	return nil
}

func (k MultiKey) GetString(path string) (string, bool) {
	return getPathString(nonNil(k.named, k.anon, k.single), path)
}

func (k MultiKey) GetInt64(path string) (int64, bool) {
	return getPathInt64(nonNil(k.named, k.anon, k.single), path)
}

func (k MultiKey) GetBig(path string) (*big.Int, bool) {
	return getPathBig(nonNil(k.named, k.anon, k.single), path)
}

func (k MultiKey) GetTime(path string) (time.Time, bool) {
	return getPathTime(nonNil(k.named, k.anon, k.single), path)
}

func (k MultiKey) GetAddress(path string) (tezos.Address, bool) {
	return getPathAddress(nonNil(k.named, k.anon, k.single), path)
}

func (k MultiKey) GetValue(path string) (interface{}, bool) {
	return getPathValue(nonNil(k.named, k.anon, k.single), path)
}

func (k MultiKey) Walk(path string, fn ValueWalkerFunc) error {
	val := nonNil(k.named, k.anon, k.single)
	if len(path) > 0 {
		var ok bool
		val, ok = getPathValue(val, path)
		if !ok {
			return nil
		}
	}
	return walkValueMap(path, val, fn)
}

func (k MultiKey) Unmarshal(val interface{}) error {
	buf, _ := json.Marshal(k)
	return json.Unmarshal(buf, val)
}

func (c *Client) ListBigmapKeys(ctx context.Context, id int64, params ContractParams) ([]BigmapKey, error) {
	keys := make([]BigmapKey, 0)
	u := params.AppendQuery(fmt.Sprintf("/explorer/bigmap/%d/keys", id))
	if err := c.get(ctx, u, nil, &keys); err != nil {
		return nil, err
	}
	return keys, nil
}
