package pool

import (
	"net"
	"sync"

	"github.com/go-i2p/logger"
	"github.com/samber/oops"
)

// PoolConnWrapper wraps a pooled connection to handle automatic release
type PoolConnWrapper struct {
	net.Conn
	pool   *ConnPool
	addr   string
	mu     sync.Mutex
	closed bool
}

// Close returns the connection to the pool instead of closing it.
// Returns an error on double-close or if the pool rejects the connection.
func (w *PoolConnWrapper) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		log.WithFields(logger.Fields{"pkg": "pool", "func": "PoolConnWrapper.Close"}).Debug("Close called on already-closed pool connection wrapper")
		return oops.Code("ALREADY_CLOSED").In("pool").
			Errorf("connection wrapper already closed")
	}
	log.WithFields(logger.Fields{"pkg": "pool", "func": "PoolConnWrapper.Close"}).Debug("Returning pooled connection")
	w.closed = true
	return w.pool.Release(w.addr, w.Conn)
}

// Discard closes the underlying connection and permanently removes it
// from the pool. Use this when the connection is known to be broken.
func (w *PoolConnWrapper) Discard() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return oops.Code("ALREADY_CLOSED").In("pool").
			Errorf("connection wrapper already closed")
	}
	log.WithFields(logger.Fields{"pkg": "pool", "func": "PoolConnWrapper.Discard"}).Debug("Discarding broken pooled connection")
	w.closed = true
	return w.pool.Remove(w.addr, w.Conn)
}
