package main

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzstats-go/tzstats"
)

func main() {
	if err := run(); err != nil {
		fmt.Println("Error:", err)
	}
}

func run() error {
	// use a placeholder calling context
	ctx := context.Background()

	// create a new SDK client
	c, err := tzstats.NewClient("https://api.tzstats.com", nil)
	if err != nil {
		return err
	}

	// fetch block
	q := c.NewBlockQuery()
	q.WithLimit(1).WithDesc()
	res, err := q.Run(ctx)
	if err != nil {
		return err
	}

	buf, _ := json.MarshalIndent(res.Rows[0], "", "  ")
	fmt.Println(string(buf))
	return nil
}
