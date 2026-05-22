package pool

import (
	"net"
	"time"
)

// PooledConn represents a connection in the pool with metadata.
// All fields are unexported to prevent callers from mutating pool state
// without holding the pool mutex. Use the accessor methods for read access.
type PooledConn struct {
	conn       net.Conn
	created    time.Time
	lastUsed   time.Time
	inUse      bool
	remoteAddr string
}

// NetConn returns the underlying network connection.
func (p *PooledConn) NetConn() net.Conn { return p.conn }

// CreatedAt returns the time the connection was added to the pool.
func (p *PooledConn) CreatedAt() time.Time { return p.created }

// LastUsedAt returns the time the connection was last returned from Get().
func (p *PooledConn) LastUsedAt() time.Time { return p.lastUsed }

// IsInUse reports whether the connection is currently checked out of the pool.
func (p *PooledConn) IsInUse() bool { return p.inUse }

// Address returns the remote address string used as the pool key.
func (p *PooledConn) Address() string { return p.remoteAddr }
