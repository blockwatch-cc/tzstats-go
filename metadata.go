// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"github.com/echa/code/iata"
	"github.com/echa/code/iso"
)

type MetadataDescriptor struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Image       string `json:"image"`
}

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
	Updated  *UpdatedMetadata  `json:"updated,omitempty"`

	// private extensions
	Extra map[string]interface{} `json:"-"`
}

func (m Metadata) ID() string {
	id := m.Address.String()
	if m.AssetId != nil {
		id += "/" + strconv.FormatInt(*m.AssetId, 10)
	}
	return id
}

func (m Metadata) IsEmpty() bool {
	return m.Alias == nil &&
		m.Baker == nil &&
		m.Payout == nil &&
		m.Asset == nil &&
		m.Location == nil &&
		m.Domain == nil &&
		m.Media == nil &&
		m.Rights == nil &&
		m.Social == nil &&
		m.Tz16 == nil &&
		m.Tz21 == nil &&
		m.Updated == nil &&
		len(m.Extra) == 0
}

func (m Metadata) Clone() Metadata {
	buf, _ := json.Marshal(m)
	var clone Metadata
	_ = json.Unmarshal(buf, &clone)
	return clone
}

func (m Metadata) Merge(d Metadata) Metadata {
	md := m
	if d.Alias != nil {
		md.Alias = d.Alias
	}
	if d.Baker != nil {
		md.Baker = d.Baker
	}
	if d.Payout != nil {
		md.Payout = d.Payout
	}
	if d.Asset != nil {
		md.Asset = d.Asset
	}
	if d.Location != nil {
		md.Location = d.Location
	}
	if d.Domain != nil {
		md.Domain = d.Domain
	}
	if d.Media != nil {
		md.Media = d.Media
	}
	if d.Rights != nil {
		md.Rights = d.Rights
	}
	if d.Social != nil {
		md.Social = d.Social
	}
	if d.Tz16 != nil {
		md.Tz16 = d.Tz16
	}
	if d.Tz21 != nil {
		md.Tz21 = d.Tz21
	}
	if d.Updated != nil {
		md.Updated = d.Updated
	}
	return md
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
			"tz21",
			"updated":
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
	Name    string         `json:"name"`
	Records []DomainRecord `json:"records,omitempty"`
}

type DomainRecord struct {
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
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Version     string       `json:"version,omitempty"`
	License     *Tz16License `json:"license,omitempty"`
	Authors     []string     `json:"authors,omitempty"`
	Homepage    string       `json:"homepage,omitempty"`
	Source      *Tz16Source  `json:"source,omitempty"`
	Interfaces  []string     `json:"interfaces,omitempty"`
	Errors      []Tz16Error  `json:"errors,omitempty"`
	Views       []Tz16View   `json:"views,omitempty"`
}

type Tz16License struct {
	Name    string `json:"name"`
	Details string `json:"details,omitempty"`
}

type Tz16Source struct {
	Tools    []string `json:"tools"`
	Location string   `json:"location,omitempty"`
}

type Tz16Error struct {
	Error     *micheline.Prim `json:"error,omitempty"`
	Expansion *micheline.Prim `json:"expansion,omitempty"`
	Languages []string        `json:"languages,omitempty"`
	View      string          `json:"view,omitempty"`
}

type Tz16View struct {
	Name            string        `json:"name"`
	Description     string        `json:"description,omitempty"`
	Pure            bool          `json:"pure,omitempty"`
	Implementations []interface{} `json:"implementations,omitempty"`
}

type Tz21Metadata struct {
	Description        string          `json:"description"`
	Minter             tezos.Address   `json:"minter"`
	Creators           []string        `json:"creators"`
	Contributors       []string        `json:"contributors"`
	Publishers         []string        `json:"publishers"`
	Date               time.Time       `json:"date"`
	BlockLevel         int64           `json:"blockLevel"`
	Type               string          `json:"type"`
	Tags               []string        `json:"tags"`
	Genres             []string        `json:"genres"`
	Language           string          `json:"language"`
	Identifier         string          `json:"identifier"`
	Rights             string          `json:"rights"`
	RightUri           string          `json:"rightUri"`
	ArtifactUri        string          `json:"artifactUri"`
	DisplayUri         string          `json:"displayUri"`
	ThumbnailUri       string          `json:"thumbnailUri"`
	ExternalUri        string          `json:"externalUri"`
	IsTransferable     bool            `json:"isTransferable"`
	IsBooleanAmount    bool            `json:"isBooleanAmount"`
	ShouldPreferSymbol bool            `json:"shouldPreferSymbol"`
	Formats            []Tz21Format    `json:"formats"`
	Attributes         []Tz21Attribute `json:"attributes"`
	Assets             []Tz21Metadata  `json:"assets"`
}

type Tz21Format struct {
	Uri        string        `json:"uri"`
	Hash       string        `json:"hash"`
	MimeType   string        `json:"mimeType"`
	FileSize   int64         `json:"fileSize"`
	FileName   string        `json:"fileName"`
	Duration   string        `json:"duration"`
	Dimensions Tz21Dimension `json:"dimensions"`
	DataRate   Tz21DataRate  `json:"dataRate"`
}

type Tz21Attribute struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type,omitempty"`
}

type Tz21Dimension struct {
	Value string `json:"value"`
	Unit  string `json:"unit"`
}

type Tz21DataRate struct {
	Value string `json:"value"`
	Unit  string `json:"unit"`
}

type UpdatedMetadata struct {
	Hash   tezos.BlockHash `json:"hash"`
	Height int64           `json:"height"`
	Time   time.Time       `json:"time"`
}

func (c *Client) ListMetadata(ctx context.Context) ([]Metadata, error) {
	resp := make([]Metadata, 0)
	if err := c.get(ctx, "/metadata", nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) GetAccountMetadata(ctx context.Context, addr tezos.Address) (Metadata, error) {
	var resp Metadata
	if err := c.get(ctx, "/metadata/"+addr.String(), nil, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *Client) GetAssetMetadata(ctx context.Context, addr tezos.Address, assetId int64) (Metadata, error) {
	var resp Metadata
	if err := c.get(ctx, fmt.Sprintf("/metadata/%s/%d", addr, assetId), nil, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *Client) CreateMetadata(ctx context.Context, metadata []Metadata) ([]Metadata, error) {
	resp := make([]Metadata, 0)
	err := c.post(ctx, "/metadata", nil, &metadata, &resp)
	return resp, err
}

func (c *Client) UpdateMetadata(ctx context.Context, alias Metadata) (Metadata, error) {
	var resp Metadata
	u := fmt.Sprintf("/metadata/%s", alias.Address)
	if alias.AssetId != nil {
		u += "/" + strconv.FormatInt(*alias.AssetId, 10)
	}
	if err := c.put(ctx, u, nil, &alias, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *Client) RemoveAccountMetadata(ctx context.Context, addr tezos.Address) error {
	return c.delete(ctx, fmt.Sprintf("/metadata/%s", addr), nil)
}

func (c *Client) RemoveAssetMetadata(ctx context.Context, addr tezos.Address, assetId int64) error {
	return c.delete(ctx, fmt.Sprintf("/metadata/%s/%d", addr, assetId), nil)
}

func (c *Client) PurgeMetadata(ctx context.Context) error {
	return c.delete(ctx, "/metadata", nil)
}

func (c *Client) Describe(ctx context.Context, ident string) (MetadataDescriptor, error) {
	var resp MetadataDescriptor
	if err := c.get(ctx, "/metadata/describe/"+ident, nil, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}
