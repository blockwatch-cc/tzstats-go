package main

import (
    "context"
    "fmt"
    "strings"

    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzstats-go/tzstats"
)

func main() {
    if err := run(); err != nil {
        fmt.Println("Error:", err)
    }
}

var nFail int

func try(name string, fn func()) {
    fmt.Printf("%s %s ", name, strings.Repeat(".", 26-len(name)))
    defer func() {
        if err := recover(); err != nil {
            fmt.Printf("FAILED\nError: %v\n", err)
            nFail++
        } else {
            fmt.Println("OK")
        }
    }()
    fn()
}

func run() error {
    // use a placeholder calling context
    ctx := context.Background()

    // create a new SDK client
    c, err := tzstats.NewClient("https://api.staging.tzstats.com", nil)
    if err != nil {
        return err
    }

    // Params
    bp := tzstats.NewBlockParams().WithRights().WithMeta()
    op := tzstats.NewOpParams().WithStorage().WithMeta()
    ap := tzstats.NewAccountParams().WithMeta()
    bkp := tzstats.NewBakerParams().WithMeta()
    cp := tzstats.NewContractParams().WithMeta()

    // -----------------------------------------------------------------
    // Common
    //

    // fetch status
    try("Status", func() {
        stat, err := c.GetStatus(ctx)
        if err != nil {
            panic(err)
        }
        if stat.Status != "synced" {
            panic(fmt.Errorf("Status is %s", stat.Status))
        }
    })

    // tip
    var tip *tzstats.Tip
    try("Tip", func() {
        tip, err = c.GetTip(ctx)
        if err != nil {
            panic(err)
        }
    })

    // protocols
    try("ListProtocols", func() {
        if p, err := c.ListProtocols(ctx); err != nil || len(p) == 0 {
            panic(fmt.Errorf("len=%d %v", len(p), err))
        }
    })

    // config
    try("GetConfig", func() {
        if _, err := c.GetConfig(ctx); err != nil {
            panic(err)
        }
    })

    // config from height
    try("GetConfigHeight", func() {
        if _, err := c.GetConfigHeight(ctx, tip.Height); err != nil {
            panic(err)
        }
    })

    // -----------------------------------------------------------------
    // Block
    //

    // block
    try("GetBlock", func() {
        if _, err := c.GetBlock(ctx, tip.Hash, bp); err != nil {
            panic(err)
        }
    })

    // block head
    try("GetHead", func() {
        if _, err := c.GetHead(ctx, bp); err != nil {
            panic(err)
        }
    })

    // block height
    try("GetBlockHeight", func() {
        if _, err := c.GetBlockHeight(ctx, tip.Height, bp); err != nil {
            panic(err)
        }
    })

    // block with ops
    try("GetBlockWithOps", func() {
        if b, err := c.GetBlockWithOps(ctx, tip.Hash, bp); err != nil || len(b.Ops) == 0 {
            panic(fmt.Errorf("len=%d %v", len(b.Ops), err))
        }
    })

    // block ops
    try("GetBlockOps", func() {
        if ops, err := c.GetBlockOps(ctx, tip.Hash, op); err != nil || len(ops) == 0 {
            panic(fmt.Errorf("len=%d %v", len(ops), err))
        }
    })

    // block table
    try("Block query", func() {
        bq := c.NewBlockQuery()
        bq.WithLimit(2).WithDesc()
        _, err = bq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // -----------------------------------------------------------------
    // Account
    //
    addr := tezos.MustParseAddress("tz1go7f6mEQfT2xX2LuHAqgnRGN6c2zHPf5c") // Main
    // addr := tezos.MustParseAddress("tz29WDVtnm7nQNS2GW45i3jPvKwa6Wkx7qos") // Ithaca

    // account
    try("GetAccount", func() {
        if _, err := c.GetAccount(ctx, addr, ap); err != nil {
            panic(err)
        }
    })

    // contracts
    try("GetAccountContracts", func() {
        if _, err := c.GetAccountContracts(ctx, addr, ap); err != nil {
            panic(err)
        }
    })

    // ops
    try("GetAccountOps", func() {
        if ops, err := c.GetAccountOps(ctx, addr, op); err != nil || len(ops) == 0 {
            panic(fmt.Errorf("len=%d %v", len(ops), err))
        }
    })

    // account table
    try("Account query", func() {
        aq := c.NewBlockQuery()
        aq.WithLimit(2).WithDesc()
        _, err = aq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // -----------------------------------------------------------------
    // Baker
    //
    addr = tezos.MustParseAddress("tz1go7f6mEQfT2xX2LuHAqgnRGN6c2zHPf5c") // main
    // addr = tezos.MustParseAddress("tz1edUYGqBtteStneTGDBrQWTFmq9cnEELiW") // ithaca

    // baker
    try("GetBaker", func() {
        if _, err := c.GetBaker(ctx, addr, bkp); err != nil {
            panic(err)
        }
    })

    // list
    try("ListBakers", func() {
        if l, err := c.ListBakers(ctx, bkp); err != nil || len(l) == 0 {
            panic(fmt.Errorf("len=%d %v", len(l), err))
        }
    })

    // votes
    try("ListBakerVotes", func() {
        if ops, err := c.ListBakerVotes(ctx, addr, op); err != nil {
            panic(fmt.Errorf("len=%d %v", len(ops), err))
        }
    })

    // endorse
    try("ListBakerEndorsements", func() {
        if ops, err := c.ListBakerEndorsements(ctx, addr, op); err != nil {
            panic(fmt.Errorf("len=%d %v", len(ops), err))
        }
    })

    // deleg
    try("ListBakerDelegations", func() {
        if ops, err := c.ListBakerDelegations(ctx, addr, op); err != nil {
            panic(fmt.Errorf("len=%d %v", len(ops), err))
        }
    })

    // rights
    try("ListBakerRights", func() {
        if _, err := c.ListBakerRights(ctx, addr, 400, bkp); err != nil {
            panic(err)
        }
    })

    // income
    try("GetBakerIncome", func() {
        if _, err := c.GetBakerIncome(ctx, addr, 400, bkp); err != nil {
            panic(err)
        }
    })

    // snapshot
    try("GetBakerSnapshot", func() {
        if _, err := c.GetBakerSnapshot(ctx, addr, 400, bkp); err != nil {
            panic(err)
        }
    })

    // rights table
    try("Rights query", func() {
        rq := c.NewCycleRightsQuery()
        rq.WithLimit(2).WithDesc()
        _, err = rq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // snapshot table
    try("Snapshot query", func() {
        sq := c.NewSnapshotQuery()
        sq.WithLimit(2).WithDesc()
        _, err = sq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // -----------------------------------------------------------------
    // Bigmap
    //

    // allocs (find a bigmap with >0 keys)
    var bmid int64 = 1
    for {
        if bm, err := c.GetBigmap(ctx, bmid, cp); err != nil {
            return fmt.Errorf("GetBigmap: %v", err)
        } else if bm.NKeys > 0 {
            break
        }
        bmid++
    }

    // keys
    try("ListBigmapKeys", func() {
        if k, err := c.ListBigmapKeys(ctx, bmid, cp); err != nil {
            panic(err)
        } else {
            if _, err := c.ListBigmapKeyUpdates(ctx, bmid, k[0].KeyHash.String(), cp); err != nil {
                panic(fmt.Errorf("ListBigmapKeyUpdates: %v", err))
            }
            // value
            if _, err := c.GetBigmapValue(ctx, bmid, k[0].KeyHash.String(), cp); err != nil {
                panic(fmt.Errorf("GetBigmapValue: %v", err))
            }
        }
    })

    // list values
    try("ListBigmapValues", func() {
        if _, err := c.ListBigmapValues(ctx, bmid, cp); err != nil {
            panic(err)
        }
    })

    // list updates
    try("ListBigmapUpdates", func() {
        if _, err := c.ListBigmapUpdates(ctx, bmid, cp); err != nil {
            panic(err)
        }
    })

    // bigmap table
    try("Bigmap query", func() {
        bmq := c.NewBigmapQuery()
        bmq.WithLimit(2).WithDesc()
        _, err = bmq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // bigmap update table
    try("Bigmap update query", func() {
        bmuq := c.NewBigmapUpdateQuery()
        bmuq.WithLimit(2).WithDesc().WithFilter(tzstats.FilterModeEqual, "bigmap_id", bmid)
        _, err = bmuq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // bigmap value table
    try("Bigmap value query", func() {
        bmvq := c.NewBigmapValueQuery()
        bmvq.WithLimit(2).WithDesc().WithFilter(tzstats.FilterModeEqual, "bigmap_id", bmid)
        _, err = bmvq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // -----------------------------------------------------------------
    // Chain
    //
    try("Chain query", func() {
        chq := c.NewChainQuery()
        chq.WithLimit(2).WithDesc()
        _, err = chq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // -----------------------------------------------------------------
    // Constant
    //
    try("Constant query", func() {
        coq := c.NewConstantQuery()
        coq.WithLimit(2).WithDesc()
        _, err = coq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // -----------------------------------------------------------------
    // Contract
    //
    addr = tezos.MustParseAddress("KT1EVPNZtekBirJhvALU5gNJS2F3ibWZXnpd") // main
    // addr = tezos.MustParseAddress("KT1UcwQtaztLSq8oufdXAtWpRTfFySCj7gFM") // ithaca

    // contract
    try("GetContract", func() {
        if _, err := c.GetContract(ctx, addr, cp); err != nil {
            panic(err)
        }
    })

    // script
    try("GetContractScript", func() {
        if _, err := c.GetContractScript(ctx, addr, cp); err != nil {
            panic(err)
        }
    })

    // storage
    try("GetContractStorage", func() {
        if _, err := c.GetContractStorage(ctx, addr, cp); err != nil {
            panic(err)
        }
    })

    // calls
    try("GetContractCalls", func() {
        if _, err := c.ListContractCalls(ctx, addr, cp); err != nil {
            panic(err)
        }
    })

    try("Contract query", func() {
        ccq := c.NewContractQuery()
        ccq.WithLimit(2).WithDesc()
        _, err = ccq.Run(ctx)
        if err != nil {
            panic(err)
        }
    })

    // -----------------------------------------------------------------
    // Gov
    //
    electionId := 11 // main
    try("GetElection", func() {
        if _, err := c.GetElection(ctx, electionId); err != nil {
            panic(err)
        }
    })

    try("ListVoters", func() {
        if _, err := c.ListVoters(ctx, electionId, 1); err != nil {
            panic(err)
        }
    })

    try("ListBallots", func() {
        if _, err := c.ListBallots(ctx, electionId, 1); err != nil {
            panic(err)
        }
    })

    // -----------------------------------------------------------------
    // Operations
    //
    try("Op query", func() {
        oq := c.NewOpQuery()
        oq.WithFilter(tzstats.FilterModeEqual, "type", "transaction").
            WithLimit(10).
            WithOrder(tzstats.OrderDesc)
        ores, err := oq.Run(ctx)
        if err != nil {
            panic(err)
        }
        if ores.Len() > 0 {
            if _, err := c.GetOp(ctx, ores.Rows[0].Hash, op); err != nil {
                panic(fmt.Errorf("GetOp: %v", err))
            }
        }
    })

    if nFail > 0 {
        fmt.Printf("%d tests have FAILED.", nFail)
    } else {
        fmt.Println("All tests have PASSED.")
    }
    return nil
}
