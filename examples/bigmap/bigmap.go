//go:build ignore
// +build ignore

package main

import (
    "blockwatch.cc/tzgo/micheline"
    "blockwatch.cc/tzgo/tezos"
    "blockwatch.cc/tzstats-go/tzstats"
    "context"
    "encoding/binary"
    "fmt"
    "io"
    "path/filepath"

    "blockwatch.cc/knoxdb/hash/xxhash"
    "blockwatch.cc/knoxdb/pack"
    _ "blockwatch.cc/knoxdb/store/bolt"
    bolt "go.etcd.io/bbolt"
)

type BigmapKV struct {
    RowId    uint64         `knox:"I,pk"        json:"row_id"`    // internal: id
    BigmapId int64          `knox:"B,i32,bloom" json:"bigmap_id"` // unique bigmap id
    Height   int64          `knox:"h,i32"       json:"height"`    // update height
    KeyId    uint64         `knox:"K,snappy"    json:"key_id"`    // xxhash(BigmapId, KeyHash)
    KeyHash  tezos.ExprHash `knox:"k,snappy"    json:"key_hash"`  // key/value bytes: binary encoded micheline.Prim Pair
}

func (m *BigmapKV) ID() uint64 {
    return m.RowId
}

func (m *BigmapKV) SetID(id uint64) {
    m.RowId = id
}

func GetKeyId(bigmapid int64, kh tezos.ExprHash) uint64 {
    var buf [40]byte
    binary.BigEndian.PutUint64(buf[:], uint64(bigmapid))
    copy(buf[8:], kh.Hash.Hash)
    return xxhash.Sum64(buf[:])
}

func Create(path string, dbOpts interface{}) (*pack.Table, error) {
    fields, err := pack.Fields(BigmapKV{})
    if err != nil {
        return nil, err
    }
    db, err := pack.CreateDatabaseIfNotExists(filepath.Dir(path), "bigmap_values", "*", dbOpts)
    if err != nil {
        return nil, fmt.Errorf("creating database: %v", err)
    }

    table, err := db.CreateTableIfNotExists("bigmap_values", fields, pack.Options{})
    if err != nil {
        db.Close()
        return nil, err
    }

    _, err = table.CreateIndexIfNotExists(
        "key",                 // name
        fields.Find("K"),      // HashId (uint64 xxhash(bigmap_id + expr-hash)
        pack.IndexTypeInteger, // sorted int, index stores uint64 -> pk value
        pack.Options{},
    )
    if err != nil {
        table.Close()
        db.Close()
        return nil, err
    }

    return table, nil
}

func main() {
    if err := run(); err != nil {
        fmt.Printf("Error: %v\n", err)
    }
}

func run() error {
    ctx := context.Background()
    c, _ := tzstats.NewClient("https://stage.tzstats.io", nil)

    table, err := Create("", &bolt.Options{
        NoGrowSync:   true, // assuming Docker + XFS
        ReadOnly:     false,
        NoSync:       true, // skip fsync (DANGEROUS on crashes)
        FreelistType: bolt.FreelistMapType,
    })
    if err != nil {
        return err
    }
    defer table.Close()

    q := c.NewBigmapUpdateQuery()
    // q.WithFilter(tzstats.FilterModeEqual, "bigmap_id", 68997)
    q.WithFilter(tzstats.FilterModeEqual, "bigmap_id", 113214)
    var (
        nkeys                         int
        nrem, n2rem, nins, nupd, nund int
        idxcnt                        int
        last                          int64
    )
    vals := make(map[string]interface{})

    for {
        upd, err := q.Run(ctx)
        if err != nil {
            return err
        }
        if upd.Len() == 0 {
            break
        }
        for i, v := range upd.Rows {
            // flush after each block
            if last != v.Height {
                table.FlushJournal(ctx)
                last = v.Height
            }

            fmt.Printf("%02d %06d %5s %s before len=%d nkeys=%d\n", i, v.Height, v.Action, v.Hash, len(vals), nkeys)
            kid := GetKeyId(v.BigmapId, v.Hash)
            switch v.Action {
            case micheline.DiffActionAlloc:
            case micheline.DiffActionCopy:
                nund++
            case micheline.DiffActionUpdate:
                // knoxdb table accounting
                // find the previous entry, key should exist
                var prev *BigmapKV
                err = pack.NewQuery("etl.bigmap.update", table).
                    AndEqual("key_id", kid).
                    WithDesc().
                    Stream(ctx, func(r pack.Row) error {
                        source := &BigmapKV{}
                        if err := r.Decode(source); err != nil {
                            return err
                        }
                        // additional check for hash collision safety
                        if source.BigmapId == v.BigmapId && source.KeyHash.Equal(v.Hash) {
                            fmt.Printf("Update: found previous table entry %d\n", source.RowId)
                            prev = source
                            return io.EOF
                        }
                        return nil
                    })
                if err != nil && err != io.EOF {
                    return fmt.Errorf("etl.bigmap.update decode: %v", err)
                }
                live := &BigmapKV{
                    BigmapId: v.BigmapId,
                    Height:   v.Height,
                    KeyId:    kid,
                    KeyHash:  v.Hash,
                }
                if prev != nil {
                    // replace
                    live.RowId = prev.RowId
                    if err := table.Update(ctx, live); err != nil {
                        return fmt.Errorf("etl.bigmap.replace: %w", err)
                    }
                    // fmt.Printf("Replaced %s keyid=%d row_id=%d\n", v.Hash, kid, prev.RowId)
                    // table.DumpJournal(os.Stdout, 0)
                } else {
                    // add
                    if err := table.Insert(ctx, live); err != nil {
                        return fmt.Errorf("etl.bigmap.insert: %w", err)
                    }
                    idxcnt++
                    // fmt.Printf("Added %s keyid=%d row_id=%d\n", v.Hash, kid, live.RowId)
                    // table.DumpJournal(os.Stdout, 0)
                }

                // separate accounting
                if _, ok := vals[v.Hash.String()]; !ok {
                    nkeys++
                    nins++
                } else {
                    nupd++
                }
                vals[v.Hash.String()] = v.Value
            case micheline.DiffActionRemove:
                // knox accounting
                var prev *BigmapKV
                err = pack.NewQuery("etl.bigmap.update", table).
                    AndEqual("key_id", kid).
                    WithDesc().
                    Stream(ctx, func(r pack.Row) error {
                        source := &BigmapKV{}
                        if err := r.Decode(source); err != nil {
                            return err
                        }
                        // additional check for hash collision safety
                        if source.BigmapId == v.BigmapId && source.KeyHash.Equal(v.Hash) {
                            // fmt.Printf("Update: found previous table entry %d\n", source.RowId)
                            prev = source
                            return io.EOF
                        }
                        return nil
                    })
                if err != nil && err != io.EOF {
                    return fmt.Errorf("etl.bigmap.update decode: %v", err)
                }
                if prev != nil {
                    if err := table.DeleteIds(ctx, []uint64{prev.RowId}); err != nil {
                        return fmt.Errorf("etl.bigmap.remove: %w", err)
                    }
                    // fmt.Printf("Deleted %s keyid=%d row_id=%d\n", v.Hash, kid, prev.RowId)
                    // table.DumpJournal(os.Stdout, 0)
                    idxcnt--
                } else {
                    // double remove is possible, actually its double update
                    // with empty value (which we translate into remove)
                    // fmt.Printf("Remove on non-existing key %d %s\n", v.BigmapId, v.Hash)
                    // table.DumpJournal(os.Stdout, 0)
                }

                // separate accounting
                if _, ok := vals[v.Hash.String()]; ok {
                    nkeys--
                    nrem++
                } else {
                    n2rem++
                }
                delete(vals, v.Hash.String())
            }
            // fmt.Printf("%d Result %s len=%d nkeys=%d\n\n", i, v.Action, len(vals), nkeys)
        }
        q.WithCursor(upd.Cursor())
    }
    fmt.Printf("n_ins=%d n_upd=%d n_rem=%d n_2rem=%d n_undef=%d nkeys=%d idx=%d\n", nins, nupd, nrem, n2rem, nund, len(vals), idxcnt)
    for n, _ := range vals {
        fmt.Printf("%s\n", n)
    }
    return nil
}
