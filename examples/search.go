// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// Search contract calls for address used in parameters
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"blockwatch.cc/tzstats-go"
	"github.com/echa/log"
)

var (
	flags   = flag.NewFlagSet("search", flag.ContinueOnError)
	verbose bool
	node    string
	index   string
)

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.StringVar(&index, "index", "http://127.0.0.1:8000", "TzStats API url")
}

func main() {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			fmt.Println("Tezos Contract Call Search")
			flags.PrintDefaults()
			os.Exit(0)
		}
		log.Fatal("Error:", err)
	}

	if verbose {
		log.SetLevel(log.LevelDebug)
		tzstats.UseLogger(log.Log)
	}

	if err := run(); err != nil {
		log.Fatal("Error:", err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := tzstats.NewClient(index, nil)
	if err != nil {
		return err
	}

	if err := searchCalls(ctx, c); err != nil {
		return err
	}

	return nil
}

// Using Explorer API
func searchCalls(ctx context.Context, c *tzstats.Client) error {
	recv := flags.Arg(0)
	addr := flags.Arg(1)
	log.Infof("Searching calls to %s for address %s", recv, addr)

	p := tzstats.NewContractParams().WithLimit(500)
	plog := log.NewProgressLogger(log.Log)
	var (
		count int
	)
	for {
		calls, err := c.GetContractCalls(ctx, recv, p)
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			break
		}
		for _, v := range calls {
			found := false
			if v.Parameters != nil {
				err := v.Parameters.Walk("", func(path string, value interface{}) error {
					if value == nil {
						return nil
					}
					if s, ok := value.(string); ok {
						found = found || s == addr
					}
					return nil
				})
				if err != nil {
					log.Errorf("%s: %v", v.Hash, err)
				}
			}
			if v.Storage != nil {
				err := v.Storage.Walk("", func(path string, value interface{}) error {
					if value == nil {
						return nil
					}
					if s, ok := value.(string); ok {
						found = found || s == addr
					}
					return nil
				})
				if err != nil {
					log.Errorf("%s: %v", v.Hash, err)
				}
			}
			for _, bmd := range v.BigmapDiff {
				err := bmd.Walk("", func(path string, value interface{}) error {
					if value == nil {
						return nil
					}
					if s, ok := value.(string); ok {
						found = found || s == addr
					}
					return nil
				})
				if err != nil {
					log.Errorf("%s: %v", v.Hash, err)
				}
			}
			count++
			if found {
				log.Infof("%s matches", v.Hash)
			}
		}
		plog.Log(len(calls))
		p = p.WithCursor(calls[len(calls)-1].RowId)
	}
	log.Infof("Processed %d calls", count)
	return nil
}

// Using Table API
func search(ctx context.Context, c *tzstats.Client) error {
	recv := flags.Arg(0)
	addr := flags.Arg(1)
	log.Infof("Searching calls to %s for address %s", recv, addr)

	q := c.NewOpQuery()
	q.Limit = 50000
	q.WithFilter(tzstats.FilterModeEqual, "type", "transaction")
	q.WithFilter(tzstats.FilterModeEqual, "receiver", recv)
	q.WithColumns("row_id", "hash", "parameters", "storage", "big_map_diff")

	plog := log.NewProgressLogger(log.Log)
	var (
		cursor uint64
		count  int
	)
	for {
		q.Cursor = cursor
		log.Infof("Fetching calls from id %d...", cursor)
		ops, err := q.Run(ctx)
		if err != nil {
			return err
		}
		if len(ops.Ops) == 0 {
			break
		}
		for _, v := range ops.Ops {
			found := false
			if v.Parameters != nil {
				err := v.Parameters.Walk("", func(path string, value interface{}) error {
					if value == nil {
						return nil
					}
					if s, ok := value.(string); ok {
						found = found || s == addr
					}
					log.Infof("%s: param %s = %v", v.Hash, path, value)
					return nil
				})
				if err != nil {
					log.Errorf("%s: %v", v.Hash, err)
				}
			}
			if v.Storage != nil {
				err := v.Storage.Walk("", func(path string, value interface{}) error {
					if value == nil {
						return nil
					}
					log.Infof("%s: storage %s = %v", v.Hash, path, value)
					if s, ok := value.(string); ok {
						found = found || s == addr
					}
					return nil
				})
				if err != nil {
					log.Errorf("%s: %v", v.Hash, err)
				}
			}
			for _, bmd := range v.BigmapDiff {
				err := bmd.Walk("", func(path string, value interface{}) error {
					if value == nil {
						return nil
					}
					log.Infof("%s: bigmap %s = %v", v.Hash, path, value)
					if s, ok := value.(string); ok {
						found = found || s == addr
					}
					return nil
				})
				if err != nil {
					log.Errorf("%s: %v", v.Hash, err)
				}
			}
			count++
			if found {
				log.Infof("%s matches", v.Hash)
			}
		}
		plog.Log(len(ops.Ops))
		cursor = ops.Ops[len(ops.Ops)-1].RowId
	}
	log.Infof("Processed %d calls", count)
	return nil
}
