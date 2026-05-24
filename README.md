# Connection Pool

The `pool` package provides connection pooling for the go-noise library. It enables connection reuse for Noise protocol connections.

## Features

- **Interface-Only Design**: Uses `net.Conn` and `net.Addr` interfaces exclusively
- **Connection Lifecycle Management**: Connections expire based on age and idle time
- **Thread-Safe Operations**: All methods safe for concurrent use
- **TOCTOU-Safe Dialing**: `GetOrDial` serializes dials per address to prevent duplicate sessions
- **Graceful Shutdown**: `Drain` waits for in-flight connections before closing
- **Usage Statistics**: Pool health and usage monitoring via `Stats` and `Snapshot`

## Quick Start

```go
package main

import (
    "context"
    "net"
    "time"
    
    "github.com/go-i2p/pool"
)

func main() {
    // Create a connection pool
    p := pool.NewConnPool(&pool.PoolConfig{
        MaxSize: 10,                // Max connections per address (0 = default 10; use Unbounded:true for no limit)
        MaxAge:  30 * time.Minute,  // Connection max lifetime
        MaxIdle: 5 * time.Minute,   // Max idle time before cleanup
    })
    defer p.Close()

    // Use GetOrDial to atomically get or create a connection
    conn, err := p.GetOrDial(context.Background(), "example.com:80", 
        func(ctx context.Context) (net.Conn, error) {
            return net.Dial("tcp", "example.com:80")
        })
    if err != nil {
        panic(err)
    }
    
    // Connection automatically returned to pool when closed
    defer conn.Close()
    
    // Use the connection...
    conn.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
}
```

## GetOrDial Pattern (Recommended for NTCP2)

`GetOrDial` atomically retrieves an existing connection or dials a new one,
serializing dials per address to prevent duplicate NTCP2 sessions:

```go
conn, err := p.GetOrDial(ctx, "10.0.0.1:15555", func(ctx context.Context) (net.Conn, error) {
    // Dial and perform any handshake/protocol negotiation
    return net.DialTimeout("tcp", "10.0.0.1:15555", 5*time.Second)
})
if err != nil {
    return err
}
defer conn.Close() // returns to pool
```

## Graceful Shutdown with Drain

Use `Drain` to wait for in-flight connections before closing the pool:

```go
// Stop accepting new work first, then:
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := p.Drain(ctx); err != nil {
    log.Printf("drain timed out: %v", err)
}
p.Close()
```

## Configuration

- `MaxSize`: Maximum connections per remote address (default: 10). A zero or negative value applies the default limit (10). To disable the per-address limit entirely, set `Unbounded: true`.
- `MaxTotal`: Maximum total connections across all addresses (0 = unlimited)
- `MaxAge`: Maximum connection lifetime (default: 30 minutes)
- `MaxIdle`: Maximum idle time before cleanup (default: 5 minutes)
- `HealthCheck`: Optional liveness probe called by `Get()` before returning a connection
- `ReadyCheck`: Optional check called by `Put()` to verify handshake completion

## Memory Growth Characteristics

The pool retains one `*sync.Mutex` per unique remote address ever passed to `GetOrDial`. These entries are **never evicted** — deleting them would reintroduce the TOCTOU race that serialized dialing is designed to prevent.

For a long-running I2P router expect approximately 24–48 bytes of retained heap per unique peer address dialed over the pool's lifetime:

| Unique peers contacted | Approximate retained memory |
|---|---|
| 1,000 | ~24–48 KB |
| 10,000 | ~240–480 KB |
| 100,000 | ~2.4–4.8 MB |

If this growth is unacceptable, create a fresh `ConnPool` periodically and migrate callers to the new instance.

## Thread Safety

All pool operations are thread-safe and support concurrent usage. The pool supports concurrent connections.
