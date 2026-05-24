package pool

import (
	"context"
	"errors"
	"time"

	"github.com/go-i2p/logger"
	"github.com/samber/oops"
)

// Drain waits for all in-use connections to be returned to the pool.
// It blocks until either all connections are idle (in_use == 0) or
// the provided context is cancelled. Use this during graceful shutdown
// to allow in-flight sessions to complete before calling Close().
//
// Drain does not prevent new connections from being checked out; it
// only waits for the current in-use count to reach zero. Callers
// should stop accepting new work before calling Drain.
func (p *ConnPool) Drain(ctx context.Context) error {
	ticker := time.NewTicker(DefaultDrainPollInterval)
	defer ticker.Stop()

	for {
		stats := p.Stats()
		if stats.InUse == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return oops.
				Code("DRAIN_TIMEOUT").
				In("pool").
				Wrapf(ctx.Err(), "drain: %d connections still in use", stats.InUse)
		case <-ticker.C:
			// Poll again.
		}
	}
}

// Snapshot returns a read-only metadata snapshot of all pooled connections
// for diagnostics, monitoring, or testing purposes.
//
// Each returned ConnSnapshot is a value copy containing only metadata fields
// (Address, CreatedAt, LastUsedAt, IsInUse). No live net.Conn handle is
// reachable from the snapshot, so callers cannot accidentally corrupt pool
// state or trigger data races by interacting with the underlying connections.
func (p *ConnPool) Snapshot() []ConnSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result []ConnSnapshot
	for _, connList := range p.conns {
		for _, pc := range connList {
			result = append(result, ConnSnapshot{
				Address:    pc.remoteAddr,
				CreatedAt:  pc.created,
				LastUsedAt: pc.lastUsed,
				IsInUse:    pc.inUse,
			})
		}
	}
	return result
}

// Close closes idle connections and prevents new connections from being added.
// In-use connections are closed when returned via Release() or Discard().
//
// Callers should call Drain() before Close() if they want to wait for
// in-flight sessions to complete. If Drain() is called concurrently with
// or after Close(), it will still correctly observe in-use connections
// and wait for them to be returned.
func (p *ConnPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}

	log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.Close"}).Debug("Closing connection pool")
	p.closed = true
	close(p.done)
	p.mu.Unlock()

	// Wait for cleanup goroutine to exit (AUDIT M-2 fix)
	p.cleanupWg.Wait()

	// Now close connections
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close only idle connections; in-use connections will be
	// closed when returned via Release() or Discard().
	// Retain in-use entries in the map so that Stats() and Drain()
	// continue to observe them until they are returned.
	var errs []error
	for addr, connList := range p.conns {
		var remaining []*PooledConn
		for _, pooledConn := range connList {
			if pooledConn.inUse {
				remaining = append(remaining, pooledConn)
			} else {
				// Clean up connRegistry for each idle connection being closed (AUDIT M5 fix).
				delete(p.connRegistry, pooledConn.conn)
				if err := pooledConn.conn.Close(); err != nil {
					wrappedErr := oops.
						Code("CLOSE_FAILED").
						In("pool").
						With("address", addr).
						Wrapf(err, "failed to close connection to %s", addr)
					errs = append(errs, wrappedErr)
				}
			}
		}
		if len(remaining) == 0 {
			delete(p.conns, addr)
		} else {
			p.conns[addr] = remaining
		}
	}

	if len(errs) > 0 {
		return oops.
			Code("CLOSE_PARTIAL").
			In("pool").
			With("failed_count", len(errs)).
			Wrapf(errors.Join(errs...), "failed to close %d connection(s)", len(errs))
	}
	return nil
}

// Stats returns a snapshot of current pool statistics. The returned PoolStats
// value is a fresh copy and can be safely read by the caller without affecting
// pool state (AUDIT M-6). Call Stats() again to get updated values.
func (p *ConnPool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	total := 0
	inUse := 0

	for _, connList := range p.conns {
		total += len(connList)
		for _, pooledConn := range connList {
			if pooledConn.inUse {
				inUse++
			}
		}
	}

	return PoolStats{
		Total:     total,
		InUse:     inUse,
		Available: total - inUse,
		Addresses: len(p.conns),
	}
}
