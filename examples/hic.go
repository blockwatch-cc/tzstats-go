package main

import (
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzstats-go"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	HIC_NFT_BIGMAP = 514
)

var (
	flags   = flag.NewFlagSet("hic", flag.ContinueOnError)
	verbose bool
	nofail  bool
	index   string
	ipfs    string
	offset  int
	store   string
)

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&nofail, "nofail", false, "no fail on IPFS error")
	flags.StringVar(&index, "index", "https://api.tzstats.com", "TzStats API URL")
	flags.StringVar(&ipfs, "ipfs", "https://ipfs.tzstats.com/ipfs", "IPFS gateway URL")
	flags.IntVar(&offset, "offset", 0, "NFT List offset")
	flags.StringVar(&store, "store", "nfts/", "path where we store metadata downloaded frm IPFS")
}

func main() {
	if err := run(); err != nil {
		fmt.Println("Error:", err)
	}
}

// IPFS metadata
type HicMetadata struct {
	Name               string               `json:"name"`
	Description        string               `json:"description,omitempty"`
	Tags               []string             `json:"tags"`
	Symbol             string               `json:"symbol"`
	ArtifactUri        string               `json:"artifactUri"`
	Creators           []string             `json:"creators"`
	Formats            []tzstats.Tz21Format `json:"formats"`
	ThumbnailUri       string               `json:"thumbnailUri"`
	Decimals           int                  `json:"decimals"`
	IsBooleanAmount    bool                 `json:"isBooleanAmount"`
	ShouldPreferSymbol bool                 `json:"shouldPreferSymbol"`
}

type HicNFT struct {
	TokenId   int               `json:"token_id,string"`
	TokenInfo map[string]string `json:"token_info"`
}

func (h HicNFT) ResolveMetadata(ctx context.Context, c *tzstats.Client) (*HicMetadata, error) {
	uri, ok := h.TokenInfo[""]
	if !ok {
		return nil, fmt.Errorf("Missing token metadata")
	}
	meta := &HicMetadata{}
	if err := c.GetIpfsData(ctx, uri, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (h HicNFT) MetadataHash() string {
	uri, ok := h.TokenInfo[""]
	if ok {
		return strings.TrimPrefix(uri, "ipfs://")
	}
	return ""
}

func run() error {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			fmt.Println("H=N NFT List")
			fmt.Println("Usage: hic [args] cmd\n")
			fmt.Println("Commands:")
			fmt.Println("  fetch        fetch metadata files from IPFS")
			fmt.Println("  list         show OBJKT metadata")
			fmt.Println("  exploit      check for metadata exploit")
			fmt.Println("\nArguments")
			flags.PrintDefaults()
			return nil
		}
		return err
	}

	// use a placeholder calling context
	ctx := context.Background()

	// create a new SDK client
	c, err := tzstats.NewClient(index, nil)
	if err != nil {
		return err
	}

	ipfsc, err := tzstats.NewClient(ipfs, nil)
	if err != nil {
		return err
	}

	switch cmd := flags.Arg(0); cmd {
	case "fetch":
		return fetch(ctx, c, ipfsc)
	case "list":
		return list(ctx, c)
	case "exploit":
		return findExploits(ctx, c, ipfsc)
	case "":
		return fmt.Errorf("Missing command")
	default:
		return fmt.Errorf("Unknown command %s", cmd)
	}

}

func findExploits(ctx context.Context, c, ipfsc *tzstats.Client) error {
	// fetch all NFTs from bigmap 511
	start := time.Now()
	var count int = offset
	params := tzstats.NewContractParams().
		WithMeta().
		WithUnpack().
		WithLimit(500).
		WithOffset(uint(offset))
	for {
		nfts, err := c.GetBigmapValues(ctx, HIC_NFT_BIGMAP, params)
		if err != nil {
			return err
		}
		if len(nfts) == 0 {
			break
		}
		for _, v := range nfts {
			var nft HicNFT
			if err := v.Unmarshal(&nft); err != nil {
				return fmt.Errorf("%v %#v", err, v.Value)
			}
		again:
			meta, err := nft.ResolveMetadata(ctx, ipfsc)
			if err != nil {
				if e, ok := tzstats.IsErrRateLimited(err); ok {
					fmt.Printf("ERR 429 - waiting %s...\n", e.Deadline())
					e.Wait(ctx)
					goto again
				}
				if nofail {
					fmt.Printf("ERR %s\n", err)
					continue
				} else {
					return err
				}
			}
			if len(meta.Creators) == 0 {
				fmt.Printf("WARN OBJKT %d has no creators\n", nft.TokenId)
				continue
			}
			if len(meta.Creators) != 1 {
				fmt.Printf("WARN OBJKT %d has %d creators\n", nft.TokenId, len(meta.Creators))
			}
			creator, err := tezos.ParseAddress(meta.Creators[0])
			if err != nil {
				fmt.Printf("WARN OBJKT %d has illegal creator address %s\n", nft.TokenId, meta.Creators[0])
				continue
			}
			if !creator.IsValid() {
				fmt.Printf("WARN OBJKT %d has empty creator address\n", nft.TokenId)
				continue
			}
			if !v.Meta.Source.Equal(creator) {
				fmt.Printf("WARN OBJKT %d creator address %s differs from source %s\n",
					nft.TokenId, creator, v.Meta.Source)
			}
		}
		count += len(nfts)
		fmt.Printf("Pos %d OK\n", count)
		params = params.WithOffset(uint(count))
	}
	fmt.Printf("Found %d Hic NFTs in %s\n", count-offset, time.Since(start))
	return nil
}

func fetch(ctx context.Context, c, ipfsc *tzstats.Client) error {
	// fetch all NFTs from bigmap 511
	start := time.Now()
	var count int = offset
	params := tzstats.NewContractParams().
		WithMeta().
		WithUnpack().
		WithLimit(500).
		WithOffset(uint(offset))
	for {
		nfts, err := c.GetBigmapValues(ctx, HIC_NFT_BIGMAP, params)
		if err != nil {
			return err
		}
		if len(nfts) == 0 {
			break
		}
		for i, v := range nfts {
			var nft HicNFT
			if err := v.Unmarshal(&nft); err != nil {
				return fmt.Errorf("%v %#v", err, v.Value)
			}
			name := filepath.Join(store, nft.MetadataHash()+".json")
			if _, err := os.Stat(name); err == nil {
				continue
			}
		again:
			meta, err := nft.ResolveMetadata(ctx, ipfsc)
			if err != nil {
				if e, ok := tzstats.IsErrRateLimited(err); ok {
					fmt.Printf("ERR 429 - waiting %s...\n", e.Deadline())
					e.Wait(ctx)
					goto again
				}
				if nofail {
					fmt.Printf("ERR %s\n", err)
					continue
				} else {
					return err
				}
			}
			buf, _ := json.Marshal(meta)
			if err := ioutil.WriteFile(name, buf, 0666); err != nil {
				return err
			}
			fmt.Printf("%-5d %-5d %s %-30s %s\n", count+i+1, nft.TokenId, v.Meta.UpdateTime.Format("2006-01-02"), meta.Name, meta.Description)
		}
		count += len(nfts)
		params = params.WithOffset(uint(count))
	}
	fmt.Printf("Found %d Hic NFTs in %s\n", count-offset, time.Since(start))
	return nil
}

func list(ctx context.Context, c *tzstats.Client) error {
	var count int = offset
	params := tzstats.NewContractParams().
		WithMeta().
		WithUnpack().
		WithLimit(500).
		WithOffset(uint(offset))
	for {
		nfts, err := c.GetBigmapValues(ctx, HIC_NFT_BIGMAP, params)
		if err != nil {
			return err
		}
		if len(nfts) == 0 {
			break
		}
		for i, v := range nfts {
			var nft HicNFT
			if err := v.Unmarshal(&nft); err != nil {
				return fmt.Errorf("%v %#v", err, v.Value)
			}
			name := filepath.Join(store, nft.MetadataHash()+".json")
			buf, err := ioutil.ReadFile(name)
			if err != nil {
				// skip
				fmt.Println("ERRO:", err)
				continue
			}
			var meta HicMetadata
			if err := json.Unmarshal(buf, &meta); err != nil {
				fmt.Println("ERRO:", err)
				continue
			}
			fmt.Printf("%-5d %-5d %s %-30s %s\n", count+i+1, nft.TokenId, v.Meta.UpdateTime.Format("2006-01-02"), meta.Name, meta.Description)
		}
		count += len(nfts)
		params = params.WithOffset(uint(count))
	}
	return nil
}
