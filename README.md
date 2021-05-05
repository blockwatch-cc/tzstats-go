## tzstats-go – Official Go SDK for the TzStats API

The official Blockwatch Go client library for TzStats. This SDK is free to use and works with the most recent version of the TzStats API v009-2021-04-16. API documentation can be found [here](https://tzstats.com/docs/api).

We will maintain this SDK on a regular basis to keep track of changes to the Tezos network and add new API features as they are released. Open source support is provided through issues in this Github repository. If you are looking for commercial support please contact us on licensing@blockwatch.cc.

This SDK is based on [TzGo](https://github.com/blockwatch-cc/tzgo), our low-level Go library for Tezos.

### TzStats-Go Versioning

As long as TzStats-Go is in beta status we will use major version 0.x. Once interfaces are stable we'll switch to 1.x. We'll use the minor version number to express compatibility with a Tezos protocol release, e.g. v0.9.0 supports all protocols up to Florence.


### Installation

```sh
go get -u blockwatch.cc/tzstats-go
```

Then import, using

```go
import (
	"blockwatch.cc/tzstats-go"
)
```

### Initializing the TzStats SDK Client

All functions are exported through a `Client` object you may is configured new client object with default configuration call:

```go
c, err := tzstats.NewClient("https://api.tzstats.com", nil)
```

The default configuration should work just fine, but if you need special timeouts, proxy or TLS settings you may use a custom `http.Client`.

```go
import (
	"crypto/tls"
	"log"
	"net"
	"net/http"

	"blockwatch.cc/tzstats-go"
)


func main() {
	hc := &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 180 * time.Second,
			}).Dial,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			}
		}
	}

	c, err := tzstats.NewClient("https://my-private-index.local:8000", hc)
	if err != nil {
		log.Fatalln(err)
	}
}
```

### Reading a single Tezos Account

*TODO*

### Reading Smart Contract Storage

*TODO*

### Listing Account Transactions

*TODO*

### Cursoring through large result sets

For efficiency reasons we limit each result list to at most 50,000 entries. List results contain `row_id` values that can be used as efficient offset pointers when fetching more data. An empty result list means there is no more data available right now.

```go
params := tzstats.NewOpParams()

for {
	resp, err := tzstats.GetAccountOps(ctx, addr, params)
	// handle error if necessary

	// handle data here

	// prepare for next iteration
	params = params.WithCursor(resp.Cursor())
}

```

### Listing bigmap keys with server-side data unwrap

*TODO*

### Listing many Bigmap keys with client-side data unwrap

*TODO*

### Decoding smart contract data into Go types

*TODO*

### Building complex Table Queries

*TODO*

### Gracefully handle rate-limits

To avoid excessive overload of our API we limit the rate at which we process your requests. This means your program may from time to time run into a rate limit. To let you gracefully handle retries by waiting until a rate limit resets, we expose the deadline and a done channel much like Go's network context does. Here's how you may use this feature:

```go
var acc *tzstats.Account
for {
	var err error
	acc, err = tzstats.GetAccount(ctx, tezos.MustParseAddress("tz1irJKkXS2DBWkU1NnmFQx1c1L7pbGg4yhk"))
	if err != nil {
		if e, ok := tzstats.IsRateLimited(err); ok {
			fmt.Printf("Rate limited, waiting for %s\n", e.Deadline())
			select {
			case <-ctx.Done():
				// wait until external context is canceled
				err = ctx.Err()
			case <-e.Done():
				// wait until rate limit reset and retry
				continue
			}
		}
	}
	break
}

// handle error and/or result here

```

## License

The MIT License (MIT) Copyright (c) 2021 Blockwatch Data Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished
to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.