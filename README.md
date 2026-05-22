# Connection Pool

The `pool` package provides connection pooling for the go-noise library. It enables connection reuse for Noise protocol connections.

## Features

- **Interface-Only Design**: Uses `net.Conn`, `net.Addr`, and `net.Listener` interfaces exclusively
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
    "github.com/go-i2p/go-noise/pool"
    "github.com/go-i2p/go-noise"
    "github.com/go-i2p/go-noise/internal"
)

func main() {
    // Create a connection pool
    p := pool.NewConnPool(&pool.PoolConfig{
        MaxSize: 10,                // Max connections per address (0 = unlimited)
        MaxAge:  30 * time.Minute,  // Connection max lifetime
        MaxIdle: 5 * time.Minute,   // Max idle time before cleanup
        ReadyCheck: func(c net.Conn) bool {
            // Only pool connections with completed handshakes
            if nc, ok := c.(*noise.NoiseConn); ok {
                return nc.GetConnectionState() == internal.StateEstablished
            }
            return true
        },
    })
    defer p.Close()

    // Use with transport functions
    noise.SetGlobalConnPool(p)

    // Example config (replace with your actual configuration)
    config := noise.NewConnConfig("XX", true)
    conn, err := noise.DialNoiseWithPool("tcp", "127.0.0.1:8080", config)
    if err != nil {
        panic(err)
    }
    // Connection automatically returned to pool when closed
    defer conn.Close()
}
```

## GetOrDial Pattern (Recommended for NTCP2)

`GetOrDial` atomically retrieves an existing connection or dials a new one,
serializing dials per address to prevent duplicate NTCP2 sessions:

```go
conn, err := p.GetOrDial(ctx, "10.0.0.1:15555", func(ctx context.Context) (net.Conn, error) {
    // Dial and perform Noise handshake
    return noise.DialNoise("tcp", "10.0.0.1:15555", config)
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

- `MaxSize`: Maximum connections per remote address (default: 10, 0 = unlimited)
- `MaxTotal`: Maximum total connections across all addresses (0 = unlimited)
- `MaxAge`: Maximum connection lifetime (default: 30 minutes)
- `MaxIdle`: Maximum idle time before cleanup (default: 5 minutes)
- `HealthCheck`: Optional liveness probe called by `Get()` before returning a connection
- `ReadyCheck`: Optional check called by `Put()` to verify handshake completion

## Thread Safety

All pool operations are thread-safe and support concurrent usage. The pool supports concurrent connections.
