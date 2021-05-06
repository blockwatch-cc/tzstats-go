// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"blockwatch.cc/tzgo/tezos"
	"github.com/echa/code/iata"
	"github.com/echa/code/iso"
)

type Metadata struct {
	// address + id together are used as unique identifier
	Address tezos.Address `json:"address"`
	AssetId *int64        `json:"asset_id,omitempty"`

	// public extensions
	Alias    *AliasMetadata    `json:"alias,omitempty"`
	Baker    *BakerMetadata    `json:"baker,omitempty"`
	Payout   *PayoutMetadata   `json:"payout,omitempty"`
	Asset    *AssetMetadata    `json:"asset,omitempty"`
	Location *LocationMetadata `json:"location,omitempty"`
	Domain   *DomainMetadata   `json:"domain,omitempty"`
	Media    *MediaMetadata    `json:"media,omitempty"`
	Rights   *RightsMetadata   `json:"rights,omitempty"`
	Social   *SocialMetadata   `json:"social,omitempty"`
	Tz16     *Tz16Metadata     `json:"tz16,omitempty"`
	Tz21     *Tz21Metadata     `json:"tz21,omitempty"`

	// private extensions
	Extra map[string]interface{} `json:"-"`
}

func (m Metadata) MarshalJSON() ([]byte, error) {
	type xMetadata Metadata
	buf, err := json.Marshal(xMetadata(m))
	if err != nil {
		return nil, err
	}
	if len(m.Extra) == 0 {
		return buf, nil
	}
	rev := make(map[string]interface{})
	json.Unmarshal(buf, &rev)
	for n, v := range m.Extra {
		rev[n] = v
	}
	return json.Marshal(rev)
}

func (m *Metadata) UnmarshalJSON(buf []byte) error {
	type xMetadata Metadata
	xm := xMetadata{}
	if err := json.Unmarshal(buf, &xm); err != nil {
		return err
	}
	*m = Metadata(xm)
	rev := make(map[string]interface{})
	_ = json.Unmarshal(buf, &rev)
	for n, v := range rev {
		switch n {
		case "address",
			"asset_id",
			"alias",
			"baker",
			"payout",
			"asset",
			"location",
			"domain",
			"media",
			"rights",
			"social",
			"tz16",
			"tz21":
			continue
		default:
			if m.Extra == nil {
				m.Extra = make(map[string]interface{})
			}
			m.Extra[n] = v
		}
	}
	return nil
}

type AliasMetadata struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Description string `json:"description,omitempty"`
	Category    string `json:"category,omitempty"`
	Logo        string `json:"logo,omitempty"`
}

type AssetMetadata struct {
	Standard string   `json:"standard,omitempty"`
	Symbol   string   `json:"symbol,omitempty"`
	Decimals int      `json:"decimals,omitempty"`
	Version  string   `json:"version,omitempty"`
	Homepage string   `json:"homepage,omitempty"`
	Genres   []string `json:"genres,omitempty"`
	Tags     []string `json:"tags,omitempty"`
}

type BakerMetadata struct {
	Status         string  `json:"status,omitempty"`
	Fee            float64 `json:"fee,omitempty"`
	PayoutDelay    bool    `json:"payout_delay,omitempty"`
	MinPayout      float64 `json:"min_payout,omitempty"`
	MinDelegation  float64 `json:"min_delegation,omitempty"`
	NonDelegatable bool    `json:"non_delegatable,omitempty"`
	Sponsored      bool    `json:"sponsored,omitempty"`
}

type PayoutMetadata struct {
	From []tezos.Address `json:"from,omitempty"`
}

type LocationMetadata struct {
	Country   iso.Country      `json:"country,omitempty"`
	City      iata.AirportCode `json:"city,omitempty"`
	Latitude  float64          `json:"lon,omitempty"`
	Longitude float64          `json:"lat,omitempty"`
	Altitude  float64          `json:"alt,omitempty"`
}

type DomainMetadata struct {
	Address tezos.Address     `json:"address"`
	Name    string            `json:"name"`
	Owner   tezos.Address     `json:"owner"`
	Expiry  time.Time         `json:"expiry"`
	Data    map[string]string `json:"data,omitempty"`
}

type MediaMetadata struct {
	ThumbnailUri string `json:"thumbnail_uri,omitempty"`
	ArtifactUri  string `json:"artifact_uri,omitempty"`
	Format       string `json:"format,omitempty"`
	Language     string `json:"language,omitempty"`
}

type RightsMetadata struct {
	Date         time.Time     `json:"date,omitempty"`
	Rights       string        `json:"rights,omitempty"`
	License      string        `json:"license,omitempty"`
	Minter       tezos.Address `json:"minter,omitempty"`
	Authors      []string      `json:"authors,omitempty"`
	Creators     []string      `json:"creators,omitempty"`
	Contributors []string      `json:"contributors,omitempty"`
	Publishers   []string      `json:"publishers,omitempty"`
}

type SocialMetadata struct {
	Twitter   string `json:"twitter,omitempty"`
	Instagram string `json:"instagram,omitempty"`
	Reddit    string `json:"reddit,omitempty"`
	Github    string `json:"github,omitempty"`
}

type Tz16Metadata struct {
	// TODO
}

type Tz21Metadata struct {
	// TODO
}

func (c *Client) ListMetadata(ctx context.Context) ([]Metadata, error) {
	resp := make([]Metadata, 0)
	if err := c.get(ctx, "/explorer/metadata", nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) GetAccountMetadata(ctx context.Context, addr tezos.Address) (Metadata, error) {
	var resp Metadata
	if err := c.get(ctx, "/explorer/metadata/"+addr.String(), nil, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *Client) GetAssetMetadata(ctx context.Context, addr tezos.Address, assetId int64) (Metadata, error) {
	var resp Metadata
	if err := c.get(ctx, fmt.Sprintf("/explorer/metadata/%s/%d", addr, assetId), nil, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *Client) CreateMetadata(ctx context.Context, metadata []Metadata) ([]Metadata, error) {
	resp := make([]Metadata, 0)
	err := c.post(ctx, "/explorer/metadata", nil, &metadata, &resp)
	return resp, err
}

func (c *Client) UpdateMetadata(ctx context.Context, alias Metadata) (Metadata, error) {
	var resp Metadata
	if err := c.put(ctx, fmt.Sprintf("/explorer/metadata/%s/%d", alias.Address, alias.AssetId), nil, &alias, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *Client) RemoveAccountMetadata(ctx context.Context, addr tezos.Address) error {
	return c.delete(ctx, fmt.Sprintf("/explorer/metadata/%s", addr), nil)
}

func (c *Client) RemoveAssetMetadata(ctx context.Context, addr tezos.Address, assetId int64) error {
	return c.delete(ctx, fmt.Sprintf("/explorer/metadata/%s/%d", addr, assetId), nil)
}

func (c *Client) PurgeMetadata(ctx context.Context) error {
	return c.delete(ctx, "/explorer/metadata", nil)
}
