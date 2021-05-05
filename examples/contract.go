// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzstats-go"
	"github.com/daviddengcn/go-colortext"
	"github.com/echa/log"
)

var (
	flags      = flag.NewFlagSet("contract", flag.ContinueOnError)
	verbose    bool
	withPrim   bool
	withUnpack bool
	nocolor    bool
	node       string
	index      string
)

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.StringVar(&node, "node", "http://127.0.0.1:8732", "Tezos node url")
	flags.StringVar(&index, "index", "http://127.0.0.1:8000", "TzIndex API url")
	flags.BoolVar(&withPrim, "prim", false, "show primitives")
	flags.BoolVar(&withUnpack, "unpack", false, "unpack packed contract data")
	flags.BoolVar(&nocolor, "no-color", false, "disable color output")
}

func main() {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			fmt.Println("Usage: contract [options] <cmd> <contract|operation>\n")
			flags.PrintDefaults()
			fmt.Println("\nCommands:")
			fmt.Println("  info      Show `contract` info")
			fmt.Println("  type      Show `contract` type")
			fmt.Println("  entry     Show `contract` entrypoints")
			fmt.Println("  storage   Show `contract` storage")
			fmt.Println("  params    Show call parameters from `operation`")
			os.Exit(0)
		}
		log.Fatal("Error:", err)
		os.Exit(1)
	}

	if verbose {
		log.SetLevel(log.LevelDebug)
		tzstats.UseLogger(log.Log)
	}

	if err := run(); err != nil {
		if e, ok := tzstats.IsApiError(err); ok {
			fmt.Printf("Error: %s: %s\n", e.Errors[0].Message, e.Errors[0].Detail)
		} else {
			fmt.Printf("Error: %v\n", err)
		}
		os.Exit(1)
	}
}

func run() error {
	if flags.NArg() < 1 {
		return fmt.Errorf("command required")
	}
	if flags.NArg() < 2 {
		return fmt.Errorf("argument required")
	}
	cmd := flags.Arg(0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := tzstats.NewClient(index, nil)
	if err != nil {
		return err
	}

	switch cmd {
	case "info":
		return getContractInfo(ctx, c, flags.Arg(1))
	case "type":
		return getContractType(ctx, c, flags.Arg(1))
	case "entry":
		return getContractEntrypoints(ctx, c, flags.Arg(1))
	case "storage":
		return getContractStorage(ctx, c, flags.Arg(1))
	case "params":
		return getContractCall(ctx, c, flags.Arg(1))
	default:
		return fmt.Errorf("unsupported command %s", cmd)
	}
}

func getContractInfo(ctx context.Context, c *tzstats.Client, addr string) error {
	_, err := tezos.ParseAddress(addr)
	if err != nil {
		return err
	}
	cc, err := c.GetContract(ctx, addr, tzstats.NewContractParams())
	if err != nil {
		return err
	}
	fmt.Println("Contract Info:")
	print(cc, 2)
	return nil
}

func getContractType(ctx context.Context, c *tzstats.Client, addr string) error {
	_, err := tezos.ParseAddress(addr)
	if err != nil {
		return err
	}
	script, err := c.GetContractScript(ctx, addr, tzstats.NewContractParams().WithPrim())
	if err != nil {
		return err
	}
	fmt.Println("Storage Type:")
	print(script.Script.StorageType(), 2)
	if withPrim {
		fmt.Println("Michelson:")
		print(script.Script.StorageType().Prim, 0)
	}
	return nil
}

func getContractEntrypoints(ctx context.Context, c *tzstats.Client, addr string) error {
	_, err := tezos.ParseAddress(addr)
	if err != nil {
		return err
	}
	cc, err := c.GetContractScript(ctx, addr, tzstats.NewContractParams().WithPrim())
	if err != nil {
		return err
	}
	fmt.Println("Entrypoints:")
	eps, err := cc.Script.Entrypoints(withPrim)
	if err != nil {
		return err
	}
	print(eps, 2)
	// if withPrim {
	// 	fmt.Println("Michelson:")
	// 	print(cc.Script.StorageType().Prim, 0)
	// }
	return nil
}

func getContractStorage(ctx context.Context, c *tzstats.Client, addr string) error {
	_, err := tezos.ParseAddress(addr)
	if err != nil {
		return err
	}
	p := tzstats.NewContractParams().WithPrim()
	cc, err := c.GetContractScript(ctx, addr, p)
	if err != nil {
		return err
	}
	store, err := c.GetContractStorage(ctx, addr, p)
	if err != nil {
		return err
	}
	fmt.Println("Storage Contents:")
	print(micheline.NewValue(cc.Script.StorageType(), store.Prim), 2)
	if withPrim {
		fmt.Println("Michelson:")
		print(store.Prim, 0)
	}
	return nil
}

func getContractCall(ctx context.Context, c *tzstats.Client, hash string) error {
	_, err := tezos.ParseOpHash(hash)
	if err != nil {
		return err
	}
	ops, err := c.GetOp(ctx, hash, tzstats.NewOpParams().WithPrim())
	if err != nil {
		return err
	}
	for _, op := range ops {
		if op.Type != tezos.OpTypeTransaction {
			continue
		}
		if !op.IsContract {
			continue
		}
		script, err := c.GetContractScript(ctx, op.Receiver.String(), tzstats.NewContractParams().WithPrim())
		if err != nil {
			return err
		}
		eps, err := script.Script.Entrypoints(withPrim)
		if err != nil {
			return err
		}
		fmt.Printf("Call Parameters for %d/%d", op.OpC, op.OpI)
		print(micheline.NewValue(eps[op.Parameters.Call].Type(), op.Parameters.Prim), 2)
		if withPrim {
			fmt.Println("Michelson:")
			print(op.Parameters.Prim, 0)
		}
	}
	return nil
}

// Color print helpers
func print(val interface{}, n int) error {
	var (
		body []byte
		err  error
	)
	if n > 0 {
		body, err = json.MarshalIndent(val, "", strings.Repeat(" ", n))
	} else {
		body, err = json.Marshal(val)
	}
	if err != nil {
		return err
	}
	if nocolor {
		os.Stdout.Write(body)
	} else {
		raw := make(map[string]interface{})
		dec := json.NewDecoder(bytes.NewBuffer(body))
		dec.UseNumber()
		dec.Decode(&raw)
		printJSON(1, raw, false)
	}
	return nil
}

func printJSON(depth int, val interface{}, isKey bool) {
	switch v := val.(type) {
	case nil:
		ct.ChangeColor(ct.Blue, false, ct.None, false)
		fmt.Print("null")
		ct.ResetColor()
	case bool:
		ct.ChangeColor(ct.Blue, false, ct.None, false)
		if v {
			fmt.Print("true")
		} else {
			fmt.Print("false")
		}
		ct.ResetColor()
	case string:
		if isKey {
			ct.ChangeColor(ct.Blue, true, ct.None, false)
		} else {
			ct.ChangeColor(ct.Yellow, false, ct.None, false)
		}
		fmt.Print(strconv.Quote(v))
		ct.ResetColor()
	case json.Number:
		ct.ChangeColor(ct.Blue, false, ct.None, false)
		fmt.Print(v)
		ct.ResetColor()
	case map[string]interface{}:

		if len(v) == 0 {
			fmt.Print("{}")
			break
		}

		var keys []string

		for h := range v {
			keys = append(keys, h)
		}

		sort.Strings(keys)

		fmt.Println("{")
		needNL := false
		for _, key := range keys {
			if needNL {
				fmt.Print(",\n")
			}
			needNL = true
			for i := 0; i < depth; i++ {
				fmt.Print("    ")
			}

			printJSON(depth+1, key, true)
			fmt.Print(": ")
			printJSON(depth+1, v[key], false)
		}
		fmt.Println("")

		for i := 0; i < depth-1; i++ {
			fmt.Print("    ")
		}
		fmt.Print("}")

	case []interface{}:

		if len(v) == 0 {
			fmt.Print("[]")
			break
		}

		fmt.Println("[")
		needNL := false
		for _, e := range v {
			if needNL {
				fmt.Print(",\n")
			}
			needNL = true
			for i := 0; i < depth; i++ {
				fmt.Print("    ")
			}

			printJSON(depth+1, e, false)
		}
		fmt.Println("")

		for i := 0; i < depth-1; i++ {
			fmt.Print("    ")
		}
		fmt.Print("]")
	default:
		fmt.Println("unknown type:", reflect.TypeOf(v))
	}
}
