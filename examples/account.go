package main

import (
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzstats-go"
	"context"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	// use default Mainnet client
	client := tzstats.DefaultClient

	// get account data and embed metadata if available
	a, err := client.GetAccount(
		context.Background(),
		tezos.MustParseAddress(os.Args[1]),
		tzstats.NewAccountParams().WithMeta(),
	)

	if err != nil {
		fmt.Println(err)
	} else {
		buf, _ := json.MarshalIndent(a, "", "  ")
		fmt.Println(string(buf))
	}
}
