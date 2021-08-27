package main

import (
	// "blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzstats-go"
	"context"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	// use default Mainnet client
	c := tzstats.DefaultClient
	ctx := context.Background()

	// get account data and embed metadata if available
	// a, err := c.GetAccount(
	// 	ctx,
	// 	tezos.MustParseAddress(os.Args[1]),
	// 	tzstats.NewAccountParams().WithMeta(),
	// )

	// fetch block
	q := c.NewAccountQuery()
	q.WithFilter(tzstats.FilterModeEqual, "address", os.Args[1])
	res, err := q.Run(ctx)

	if err != nil {
		fmt.Println(err)
	} else {
		a := res.Rows[0]
		buf, _ := json.MarshalIndent(a, "", "  ")
		fmt.Println(string(buf))
	}
}
