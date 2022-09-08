// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"blockwatch.cc/tzgo/contract"
	"blockwatch.cc/tzgo/tezos"
	"github.com/echa/code/iata"
	"github.com/echa/code/iso"
)

var Schemas = map[string]func() interface{}{
	"alias":    func() interface{} { return new(AliasMetadata) },
	"baker":    func() interface{} { return new(BakerMetadata) },
	"payout":   func() interface{} { return new(PayoutMetadata) },
	"asset":    func() interface{} { return new(AssetMetadata) },
	"dex":      func() interface{} { return new(DexMetadata) },
	"location": func() interface{} { return new(LocationMetadata) },
	"domain":   func() interface{} { return new(DomainMetadata) },
	"media":    func() interface{} { return new(MediaMetadata) },
	"rights":   func() interface{} { return new(RightsMetadata) },
	"social":   func() interface{} { return new(SocialMetadata) },
	"tz16":     func() interface{} { return new(contract.Tz16) },
	"tz21":     func() interface{} { return new(Tz21Metadata) },
	"updated":  func() interface{} { return new(UpdatedMetadata) },
}

type MetadataDescriptor struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Image       string `json:"image"`
}

type Metadata struct {
	// address + id together are used as unique identifier
	Address  tezos.Address          `json:"address"`
	AssetId  *int64                 `json:"asset_id,omitempty"`
	Contents map[string]interface{} `json:"-"`
}

func (m Metadata) ID() string {
	id := m.Address.String()
	if m.AssetId != nil {
		id += "/" + strconv.FormatInt(*m.AssetId, 10)
	}
	return id
}

func (m Metadata) Has(name string) bool {
	if m.Contents == nil {
		return false
	}
	v, ok := m.Contents[name]
	return ok && v != nil
}

func (m Metadata) Get(name string) interface{} {
	if m.Contents != nil {
		v, ok := m.Contents[name]
		if ok {
			return v
		}
	}
	s, ok := Schemas[name]
	if ok {
		return s()
	}
	return make(map[string]interface{})
}

func (m *Metadata) Set(name string, data interface{}) {
	if m.Contents == nil {
		m.Contents = make(map[string]interface{})
	}
	m.Contents[name] = data
}

func (m *Metadata) Delete(name string) {
	if m.Contents != nil {
		delete(m.Contents, name)
	}
}

func (m Metadata) IsEmpty() bool {
	return len(m.Contents) == 0
}

func (m Metadata) Clone() Metadata {
	buf, _ := json.Marshal(m)
	var clone Metadata
	_ = json.Unmarshal(buf, &clone)
	return clone
}

func (m Metadata) Merge(d Metadata) Metadata {
	md := m
	for n, v := range d.Contents {
		if v == nil {
			continue
		}
		md.Contents[n] = v
	}
	return md
}

func (m Metadata) MarshalJSON() ([]byte, error) {
	out := make(map[string]interface{})
	for n, v := range m.Contents {
		out[n] = v
	}
	out["address"] = m.Address
	if m.AssetId != nil {
		out["asset_id"] = *m.AssetId
	}
	return json.Marshal(out)
}

func (m *Metadata) UnmarshalJSON(buf []byte) error {
	type xMetadata map[string]json.RawMessage
	xm := xMetadata{}
	if err := json.Unmarshal(buf, &xm); err != nil {
		return err
	}
	for n, v := range xm {
		var err error
		switch n {
		case "address":
			err = json.Unmarshal(v, &m.Address)
		case "asset_id":
			err = json.Unmarshal(v, &m.AssetId)
		default:
			var data interface{}
			schema, ok := Schemas[n]
			if ok {
				data = schema()
			} else {
				data = make(map[string]interface{})
			}
			err = json.Unmarshal(v, &data)
			if err == nil {
				m.Set(n, data)
			}
		}
		if err != nil {
			return fmt.Errorf("Reading %q: %v", n, err)
		}
	}
	return nil
}

func (m *Metadata) Alias() *AliasMetadata {
	name := "alias"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*AliasMetadata)
}

func (m *Metadata) Baker() *BakerMetadata {
	name := "baker"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*BakerMetadata)
}

func (m *Metadata) Payout() *PayoutMetadata {
	name := "payout"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*PayoutMetadata)
}

func (m *Metadata) Asset() *AssetMetadata {
	name := "asset"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*AssetMetadata)
}

func (m *Metadata) Dex() *DexMetadata {
	name := "dex"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*DexMetadata)
}

func (m *Metadata) Location() *LocationMetadata {
	name := "location"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*LocationMetadata)
}

func (m *Metadata) Domain() *DomainMetadata {
	name := "domain"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*DomainMetadata)
}

func (m *Metadata) Media() *MediaMetadata {
	name := "media"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*MediaMetadata)
}

func (m *Metadata) Rights() *RightsMetadata {
	name := "rights"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*RightsMetadata)
}

func (m *Metadata) Social() *SocialMetadata {
	name := "social"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*SocialMetadata)
}

func (m *Metadata) Tz16() *contract.Tz16 {
	name := "tz16"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*contract.Tz16)
}

func (m *Metadata) Tz21() *Tz21Metadata {
	name := "tz21"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*Tz21Metadata)
}

func (m *Metadata) Updated() *UpdatedMetadata {
	name := "updated"
	model, ok := m.Contents[name]
	if !ok {
		model = Schemas[name]()
		m.Set(name, model)
	}
	return model.(*UpdatedMetadata)
}

type AliasMetadata struct {
	Name        string   `json:"name"`
	Kind        string   `json:"kind"`
	Description string   `json:"description,omitempty"`
	Category    string   `json:"category,omitempty"`
	Logo        string   `json:"logo,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

type AssetMetadata struct {
	Standard string `json:"standard,omitempty"`
	Symbol   string `json:"symbol,omitempty"`
	Decimals int    `json:"decimals,omitempty"`
	Version  string `json:"version,omitempty"`
	Homepage string `json:"homepage,omitempty"`
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
	Website   string `json:"website,omitempty"`
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

// AMM and other decentralized exchanges
type DexMetadata struct {
	Kind       string    `json:"kind"`                  // quipu_v1, quipu_token, quipu_v2, vortex, ..
	TradingFee float64   `json:"trading_fee,omitempty"` // trading fee
	ExitFee    float64   `json:"exit_fee,omitempty"`    // remove liquidity fee
	Url        string    `json:"url,omitempty"`         // homepage
	Pairs      []DexPair `json:"pairs"`                 // trading pairs
}

type DexPair struct {
	PairId *int64   `json:"pair_id,omitempty"` // 0 when single pool dex
	TokenA DexToken `json:"token_a"`
	TokenB DexToken `json:"token_b"`
	Url    string   `json:"url,omitempty"` // deep link
}

type DexToken struct {
	Type    string `json:"type"`               // tez, fa12, fa2
	Address string `json:"address,omitempty"`  // token ledger, only used for FA*
	TokenId *int64 `json:"token_id,omitempty"` // only used for FA2
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

func (c *Client) GetMetadataSchema(ctx context.Context, name string) (json.RawMessage, error) {
	var msg json.RawMessage
	if err := c.get(ctx, "/metadata/schemas/"+name, nil, &msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *Client) GetAllMetadataSchemas(ctx context.Context) (map[string]json.RawMessage, error) {
	schemas := make(map[string]json.RawMessage)
	names := make([]string, 0)
	if err := c.get(ctx, "/metadata/schemas", nil, &names); err != nil {
		return nil, err
	}
	for _, name := range names {
		s, err := c.GetMetadataSchema(ctx, name)
		if err != nil {
			return nil, err
		}
		schemas[name] = s
	}
	return schemas, nil
}
