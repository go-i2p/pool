package pool

import (
	"errors"
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
// If Release fails with a POOL_CLOSED error, the connection was already closed
// inside Release; Close returns that error without calling Close() again (L-4).
// For any other Release failure, the connection is closed defensively and the
// error from w.Conn.Close() is returned.
func (w *PoolConnWrapper) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		log.WithFields(logger.Fields{"pkg": "pool", "func": "PoolConnWrapper.Close"}).Debug("Close called on already-closed pool connection wrapper")
		return oops.Code("ALREADY_CLOSED").In("pool").
			Errorf("connection wrapper already closed")
	}
	w.closed = true
	if err := w.pool.Release(w.addr, w.Conn); err != nil {
		// Check whether the pool was closed and already closed the connection
		// inside Release. If so, calling Close() again would double-close the
		// underlying net.Conn (L-4). We return the POOL_CLOSED error directly.
		var oopsErr oops.OopsError
		if errors.As(err, &oopsErr) && oopsErr.Code() == "POOL_CLOSED" {
			log.WithFields(logger.Fields{
				"pkg": "pool", "func": "PoolConnWrapper.Close",
			}).Debug("Pool closed; connection already closed by Release")
			return err
		}
		// Other Release failure — defensively close the underlying connection
		// to prevent resource leaks and log the pool-state anomaly.
		log.WithFields(logger.Fields{
			"pkg": "pool", "func": "PoolConnWrapper.Close",
		}).Warnf("Release failed (pool state anomaly): %v", err)
		return w.Conn.Close()
	}
	log.WithFields(logger.Fields{"pkg": "pool", "func": "PoolConnWrapper.Close"}).Debug("Returned pooled connection to pool")
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
