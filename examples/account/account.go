package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzstats-go/tzstats"
)

func main() {
	err := runExplorer()
	// err := runTable()
	if err != nil {
		fmt.Println("Error:", err)
	}
}

func runExplorer() error {
	// use explorer API to get account info and embed metadata if available
	a, err := tzstats.DefaultClient.GetAccount(
		context.Background(),
		tezos.MustParseAddress(os.Args[1]),
		tzstats.NewAccountParams().WithMeta(),
	)
	if err != nil {
		return err
	}
	buf, _ := json.MarshalIndent(a, "", "  ")
	fmt.Println(string(buf))
	return nil
}

func runTable() error {
	// use table API to get raw account info
	q := tzstats.DefaultClient.NewAccountQuery()
	q.WithFilter(tzstats.FilterModeEqual, "address", os.Args[1])
	res, err := q.Run(context.Background())
	if err != nil {
		return err
	}
	a := res.Rows[0]
	buf, _ := json.MarshalIndent(a, "", "  ")
	fmt.Println(string(buf))
	return nil
}
