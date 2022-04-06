// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzstats

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type Filter struct {
	Mode   FilterMode
	Column string
	Value  interface{}
}

type FilterList []Filter

func (l *FilterList) Add(mode FilterMode, col string, val ...interface{}) {
	*l = append(*l, Filter{
		Mode:   mode,
		Column: col,
		Value:  ToString(val),
	})
}

type FilterMode string

const (
	FilterModeEqual    FilterMode = "eq"
	FilterModeNotEqual FilterMode = "ne"
	FilterModeGt       FilterMode = "gt"
	FilterModeGte      FilterMode = "gte"
	FilterModeLt       FilterMode = "lt"
	FilterModeLte      FilterMode = "lte"
	FilterModeIn       FilterMode = "in"
	FilterModeNotIn    FilterMode = "nin"
	FilterModeRange    FilterMode = "rg"
	FilterModeRegexp   FilterMode = "re"
)

type OrderType string

const (
	OrderAsc  OrderType = "asc"
	OrderDesc OrderType = "desc"
)

type FormatType string

const (
	FormatJSON FormatType = "json"
	FormatCSV  FormatType = "csv"
)

type TableQuery interface {
	WithFilter(mode FilterMode, col string, val ...interface{}) TableQuery
	ReplaceFilter(mode FilterMode, col string, val ...interface{}) TableQuery
	ResetFilter() TableQuery
	WithLimit(limit int) TableQuery
	WithColumns(cols ...string) TableQuery
	WithOrder(order OrderType) TableQuery
	WithDesc() TableQuery
	WithVerbose() TableQuery
	WithQuiet() TableQuery
	WithFormat(format FormatType) TableQuery
	WithPrim() TableQuery
	Check() error
	Url() string
}

type tableQuery struct {
	Params
	client  *Client
	Table   string     // "op", "block", "chain", "flow"
	Format  FormatType // "json", "csv"
	Columns []string
	Limit   int
	Cursor  uint64
	Verbose bool
	Prim    bool
	Filter  FilterList
	Order   OrderType // asc, desc
	// OrderBy string // column name
	// Sort string // asc/desc
}

func newTableQuery(name string) tableQuery {
	return tableQuery{
		Params: NewParams(),
		Table:  name,
		Filter: make(FilterList, 0),
		Order:  OrderAsc,
	}
}

func (q *tableQuery) WithFilter(mode FilterMode, col string, val ...interface{}) TableQuery {
	q.Filter.Add(mode, col, val)
	return q
}

func (q *tableQuery) ReplaceFilter(mode FilterMode, col string, val ...interface{}) TableQuery {
	for i, v := range q.Filter {
		if v.Column == col {
			q.Filter[i].Mode = mode
			q.Filter[i].Value = ToString(val)
			return q
		}
	}
	q.Filter.Add(mode, col, val)
	return q
}

func (q *tableQuery) ResetFilter() TableQuery {
	q.Filter = q.Filter[:0]
	return q
}

func (q *tableQuery) WithLimit(limit int) TableQuery {
	q.Limit = limit
	return q
}

func (q *tableQuery) WithColumns(cols ...string) TableQuery {
	q.Columns = cols
	return q
}

func (q *tableQuery) WithOrder(order OrderType) TableQuery {
	q.Order = order
	return q
}

func (q *tableQuery) WithDesc() TableQuery {
	q.Order = OrderDesc
	return q
}

func (q *tableQuery) WithVerbose() TableQuery {
	q.Verbose = true
	return q
}

func (q *tableQuery) WithQuiet() TableQuery {
	q.Verbose = false
	return q
}

func (q *tableQuery) WithFormat(format FormatType) TableQuery {
	q.Format = format
	return q
}

func (q *tableQuery) WithPrim() TableQuery {
	q.Prim = true
	return q
}

func (q *tableQuery) WithCursor(c uint64) TableQuery {
	q.Cursor = c
	return q
}

func (p tableQuery) Check() error {
	if err := p.Params.Check(); err != nil {
		return err
	}
	if p.Table == "" {
		return fmt.Errorf("empty table name")
	}
	for _, v := range p.Filter {
		if v.Column == "" {
			return fmt.Errorf("empty filter column name")
		}
		if v.Mode == "" {
			return fmt.Errorf("invalid filter mode for filter column '%s'", v.Column)
		}
		if v.Value == nil {
			return fmt.Errorf("empty value for filter column '%s'", v.Column)
		}
	}
	switch p.Format {
	case "json", "csv", "":
		// OK
	default:
		return fmt.Errorf("unsupported format '%s'", p.Format)
	}
	return nil
}

func (p tableQuery) Url() string {
	if p.Cursor > 0 {
		p.Params.Query.Set("cursor", strconv.FormatUint(p.Cursor, 10))
	}
	if p.Limit > 0 && p.Params.Query.Get("limit") == "" {
		p.Params.Query.Set("limit", strconv.Itoa(p.Limit))
	}
	if len(p.Columns) > 0 && p.Params.Query.Get("columns") == "" {
		p.Params.Query.Set("columns", strings.Join(p.Columns, ","))
	}
	if p.Verbose {
		p.Params.Query.Set("verbose", "true")
	}
	for _, v := range p.Filter {
		p.Params.Query.Set(v.Column+"."+string(v.Mode), ToString(v.Value))
	}
	p.Params.Query.Set("order", string(p.Order))
	format := p.Format
	if format == "" {
		format = FormatJSON
	}
	return p.Params.Url("tables/" + p.Table + "." + string(format))
}

func (c *Client) QueryTable(ctx context.Context, q TableQuery, result interface{}) error {
	if err := q.Check(); err != nil {
		return err
	}
	err := c.get(ctx, q.Url(), nil, result)
	return err
}

func (c *Client) StreamTable(ctx context.Context, q TableQuery, w io.Writer) (StreamResponse, error) {
	if err := q.Check(); err != nil {
		return StreamResponse{}, err
	}
	// call with a non-nil header to indicate we expect response headers and trailers
	headers := make(http.Header)
	// signal upstream we accept trailers (required for some proxies to forward)
	headers.Add("TE", "trailers")
	if err := c.get(ctx, q.Url(), headers, w); err != nil {
		return StreamResponse{}, err
	}
	return NewStreamResponse(headers)
}

func getTableColumn(data []byte, columns []string, name string) (string, bool) {
	idx := colIndex(columns, name)
	if idx < 0 {
		return "", false
	}

	var (
		skipJson int = -1
		skip     bool
		escape   bool
		field    int
		res      []byte
	)
	for _, v := range data {
		if field > idx {
			break
		}
		if field == idx && v != ',' {
			res = append(res, v)
		}
		if escape {
			escape = false
			continue
		}
		switch v {
		case '[', '{':
			skipJson++
		case ']', '}':
			skipJson--
		}
		switch v {
		case '\\':
			escape = true
		case '"':
			skip = !skip
		case ',':
			if !skip && skipJson == 0 {
				field++
			}
		}
	}
	return strings.Trim(string(res), "\""), true
}

func colIndex(columns []string, name string) int {
	for i, v := range columns {
		if v != name {
			continue
		}
		return i
	}
	return -1
}
