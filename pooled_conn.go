package pool

import (
	"net"
	"time"
)

// ConnSnapshot is a read-only metadata copy of a pooled connection's state
// at a point in time. It carries no live resource handles and is safe to
// store, compare, and pass freely across goroutines without risk of corrupting
// pool state. Use Snapshot() to obtain values of this type.
type ConnSnapshot struct {
	// Address is the remote address string used as the pool key.
	Address string
	// CreatedAt is the time the connection was added to the pool.
	CreatedAt time.Time
	// LastUsedAt is the time the connection was last returned from Get().
	LastUsedAt time.Time
	// IsInUse reports whether the connection is currently checked out.
	IsInUse bool
}

// PooledConn represents a connection in the pool with metadata.
// All fields are unexported to prevent callers from mutating pool state
// without holding the pool mutex. PooledConn is an internal bookkeeping
// type; use Snapshot() to get a safe, read-only ConnSnapshot for diagnostics.
type PooledConn struct {
	conn       net.Conn
	created    time.Time
	lastUsed   time.Time
	inUse      bool
	remoteAddr string
}

// NetConn returns the underlying network connection.
//
// Deprecated: NetConn is retained for internal pool use only. Snapshot()
// now returns []ConnSnapshot which carries no live net.Conn handle. Do not
// call Close(), Write(), or Read() on this value from external code.
func (p *PooledConn) NetConn() net.Conn { return p.conn }
