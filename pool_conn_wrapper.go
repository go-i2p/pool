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
// Returns an error on double-close or if the underlying connection close fails.
// If Release fails internally (e.g., CONNECTION_NOT_FOUND due to stale pool
// state), the underlying connection is closed defensively, the Release error
// is logged at WARN level, and nil is returned — because from the caller's
// perspective the connection is gone (AUDIT L5 fix). A non-nil error is
// returned only when the underlying w.Conn.Close() itself fails.
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
	if err := w.pool.Release(w.addr, w.Conn); err != nil {
		// Release failed — defensively close the underlying connection to
		// prevent resource leaks, log the pool-state anomaly, and return nil
		// so callers using `defer conn.Close()` don't see spurious errors.
		log.WithFields(logger.Fields{
			"pkg": "pool", "func": "PoolConnWrapper.Close",
		}).Warnf("Release failed (pool state anomaly): %v", err)
		return w.Conn.Close()
	}
	return nil
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
