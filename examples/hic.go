package main

import (
	"blockwatch.cc/tzstats-go"
	"context"
	"flag"
	"fmt"
	"os"
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
)

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&nofail, "nofail", false, "no fail on IPFS error")
	flags.StringVar(&index, "index", "https://api.tzstats.com", "TzStats API URL")
	flags.StringVar(&ipfs, "ipfs", "https://ipfs.tzstats.com/ipfs", "IPFS gateway URL")
	flags.IntVar(&offset, "offset", 0, "NFT List offset")
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

func run() error {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			fmt.Println("H=N NFT List")
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
			fmt.Printf("%-5d %-5d %s %-30s %s\n", count+i+1, nft.TokenId, v.Meta.UpdateTime.Format("2006-01-02"), meta.Name, meta.Description)
		}
		count += len(nfts)
		params = params.WithOffset(uint(count))
	}
	fmt.Printf("Found %d Hic NFTs in %s\n", count-offset, time.Since(start))
	return nil
}
