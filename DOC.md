# pool
--
    import "github.com/go-i2p/go-noise/pool"



## Usage

#### type ConnPool

```go
type ConnPool struct {
}
```

ConnPool manages a pool of reusable connections for performance optimization. It
only uses interface types (net.Conn, net.Addr) for maximum compatibility.

#### func  NewConnPool

```go
func NewConnPool(config *PoolConfig) *ConnPool
```
NewConnPool creates a new connection pool with the given configuration

#### func (*ConnPool) Close

```go
func (p *ConnPool) Close() error
```
Close closes idle connections and prevents new connections from being added.
In-use connections are closed when returned via Release() or Discard().

Callers should call Drain() before Close() if they want to wait for
in-flight sessions to complete. If Drain() is called concurrently with
or after Close(), it will still correctly observe in-use connections
and wait for them to be returned.

#### func (*ConnPool) Drain

```go
func (p *ConnPool) Drain(ctx context.Context) error
```
Drain waits for all in-use connections to be returned to the pool.
It blocks until either all connections are idle (in_use == 0) or
the provided context is cancelled. Use this during graceful shutdown
to allow in-flight sessions to complete before calling Close().

Drain does not prevent new connections from being checked out; it
only waits for the current in-use count to reach zero. Callers
should stop accepting new work before calling Drain.

#### func (*ConnPool) Get

```go
func (p *ConnPool) Get(remoteAddr string) net.Conn
```
Get retrieves a connection from the pool for the given remote address. Returns
nil if no suitable connection is available.

#### func (*ConnPool) GetOrDial

```go
func (p *ConnPool) GetOrDial(ctx context.Context, remoteAddr string, dial func(ctx context.Context) (net.Conn, error)) (net.Conn, error)
```
GetOrDial atomically retrieves an idle connection for remoteAddr or, if none
is available, calls dial to create a new one. The dial function is called
outside the pool lock so it may perform blocking I/O (e.g., TCP connect +
Noise handshake), but only one goroutine at a time will dial for a given
remoteAddr. This prevents the TOCTOU race where multiple goroutines
simultaneously discover an empty pool and each dial a fresh connection to
the same NTCP2 router.

The returned connection is wrapped in a PoolConnWrapper. If dial succeeds,
the new connection is added to the pool and checked out in a single
atomic step.

If ctx is cancelled before dial completes, GetOrDial returns ctx.Err().

#### func (*ConnPool) Put

```go
func (p *ConnPool) Put(conn net.Conn) error
```
Put adds a connection to the pool for reuse.

Callers must only Put() connections whose Noise handshake has been
completed. If a ReadyCheck callback is configured in PoolConfig, it is
called before pooling; the connection is rejected (closed) if the check
returns false. Without a ReadyCheck, it is the caller's responsibility
to ensure the connection is in a usable state.

#### func (*ConnPool) Release

```go
func (p *ConnPool) Release(remoteAddr string, conn net.Conn) error
```
Release marks a connection as no longer in use, making it available for reuse.
Returns an error if the pool is closed or the connection is not found.

If conn is a *PoolConnWrapper, the wrapper is marked closed so that a
subsequent call to wrapper.Close() returns an ALREADY_CLOSED error instead
of issuing a second release (preventing a double-release vulnerability).

#### func (*ConnPool) Remove

```go
func (p *ConnPool) Remove(remoteAddr string, conn net.Conn) error
```
Remove closes a connection and permanently removes it from the pool.
Use this when a connection is known to be broken.

Returns CONNECTION_NOT_FOUND if the connection was not in the pool
for the given address (the connection is still closed in this case
to avoid resource leaks). Returns nil on success.

#### func (*ConnPool) Snapshot

```go
func (p *ConnPool) Snapshot() []*PooledConn
```
Snapshot returns a point-in-time copy of all pooled connections' metadata.
Each returned PooledConn is a shallow copy — the underlying net.Conn is
shared with the pool, so callers must not Close or Write on it. Use
Snapshot for diagnostics, monitoring, or testing where you need to inspect
pool state without modifying it.

#### func (*ConnPool) Stats

```go
func (p *ConnPool) Stats() map[string]int
```
Stats returns pool statistics

#### type PoolConfig

```go
type PoolConfig struct {
	// MaxSize is the maximum number of connections per remote address.
	// A value of 0 means no per-address limit is enforced.
	MaxSize int
	// MaxTotal is the maximum total number of connections across all addresses.
	// A zero value means no global limit is enforced.
	MaxTotal int
	// MaxAge is the maximum age of a connection before it is closed.
	MaxAge time.Duration
	// MaxIdle is the maximum idle time before a connection is closed.
	MaxIdle time.Duration
	// HealthCheck is an optional callback to probe connection liveness
	// before returning it from Get(). Return true if healthy.
	HealthCheck func(net.Conn) bool
	// ReadyCheck is an optional callback invoked by Put() to verify that a
	// connection is ready for reuse (e.g., that a Noise handshake has been
	// completed). Return true if the connection is ready to be pooled.
	// When nil, all connections are accepted by Put().
	ReadyCheck func(net.Conn) bool
}
```

PoolConfig configures a connection pool

#### type PoolConnWrapper

```go
type PoolConnWrapper struct {
	net.Conn
}
```

PoolConnWrapper wraps a pooled connection to handle automatic release.
Calling Close() returns the connection to the pool; calling Discard() removes it
permanently.

#### func (*PoolConnWrapper) Close

```go
func (w *PoolConnWrapper) Close() error
```
Close returns the connection to the pool instead of closing it.
Returns an error on double-close or if the pool rejects the connection.

#### func (*PoolConnWrapper) Discard

```go
func (w *PoolConnWrapper) Discard() error
```
Discard closes the underlying connection and permanently removes it from the
pool. Use this when the connection is known to be broken.

#### type PooledConn

```go
type PooledConn struct {
}
```

PooledConn represents a connection in the pool with metadata. All fields are
unexported to prevent callers from mutating pool state without holding the pool
mutex. Use the accessor methods for read access.

#### func (*PooledConn) Address

```go
func (p *PooledConn) Address() string
```
Address returns the remote address string used as the pool key.

#### func (*PooledConn) CreatedAt

```go
func (p *PooledConn) CreatedAt() time.Time
```
CreatedAt returns the time the connection was added to the pool.

#### func (*PooledConn) IsInUse

```go
func (p *PooledConn) IsInUse() bool
```
IsInUse reports whether the connection is currently checked out of the pool.

#### func (*PooledConn) LastUsedAt

```go
func (p *PooledConn) LastUsedAt() time.Time
```
LastUsedAt returns the time the connection was last returned from Get().

#### func (*PooledConn) NetConn

```go
func (p *PooledConn) NetConn() net.Conn
```
NetConn returns the underlying network connection.
