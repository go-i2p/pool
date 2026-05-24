package pool

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/go-i2p/logger"
	"github.com/samber/oops"
)

// Get retrieves a connection from the pool for the given remote address.
// Returns nil if no suitable connection is available.
//
// Get delegates to GetWithContext with a background context. Use
// GetWithContext directly if the caller has a deadline or cancellation
// requirement, so that a blocking HealthCheck can be interrupted.
func (p *ConnPool) Get(remoteAddr string) PooledConnection {
	return p.GetWithContext(context.Background(), remoteAddr)
}

// GetWithContext retrieves a connection from the pool for the given remote address,
// respecting context cancellation during health check execution. Returns nil if no
// suitable connection is available or if the context is cancelled.
//
// This method is recommended over Get() for applications with request deadlines or
// timeout requirements, as it prevents indefinite blocking if the health check
// callback performs blocking I/O (AUDIT M-1 fix).
//
// If ctx is cancelled before the health check completes, the candidate connection
// is returned to the pool and nil is returned.
func (p *ConnPool) GetWithContext(ctx context.Context, remoteAddr string) PooledConnection {
	var candidate *PooledConn
	var toClose []*PooledConn

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.GetWithContext"}).Debug("GetWithContext called on closed pool")
		return nil
	}

	connList, exists := p.conns[remoteAddr]
	if !exists || len(connList) == 0 {
		p.mu.Unlock()
		return nil
	}

	// Find an available and valid connection (AUDIT L2 fix: uses shared helper).
	candidate, toClose = p.findCandidate(remoteAddr, connList)
	p.mu.Unlock()

	// Close expired connections outside the lock
	for _, pc := range toClose {
		if err := pc.conn.Close(); err != nil {
			log.WithFields(logger.Fields{
				"pkg": "pool", "func": "ConnPool.GetWithContext",
				"remote_addr": pc.remoteAddr,
			}).Warnf("failed to close expired connection: %v", err)
		}
	}

	// Run health check outside the lock if we have a candidate
	if candidate != nil {
		// Check context before running health check
		if err := ctx.Err(); err != nil {
			// Context already cancelled, return connection to pool
			p.mu.Lock()
			candidate.inUse = false
			p.mu.Unlock()
			return nil
		}

		// Wrap health check with panic recovery and context cancellation (AUDIT M-1 fix)
		healthy := true
		if p.healthCheck != nil {
			// Run health check in a goroutine with context cancellation
			healthResult := make(chan bool, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.WithFields(logger.Fields{
							"pkg": "pool", "func": "ConnPool.GetWithContext",
							"remote_addr": candidate.remoteAddr,
						}).Errorf("HealthCheck panicked: %v", r)
						healthResult <- false
					}
				}()
				healthResult <- p.healthCheck(candidate.conn)
			}()

			select {
			case healthy = <-healthResult:
				// Health check completed
			case <-ctx.Done():
				// Context cancelled during health check (AUDIT H2 + M6 fix).
				// Close the candidate connection to interrupt any blocking I/O
				// inside the health-check goroutine, so the goroutine unblocks
				// and the buffered channel drains quickly (AUDIT M6: prevents
				// indefinite goroutine accumulation when health checks block).
				// After draining we remove the connection from the pool (it is
				// now closed and cannot be reused).
				log.WithFields(logger.Fields{
					"pkg": "pool", "func": "ConnPool.GetWithContext",
					"remote_addr": candidate.remoteAddr,
				}).Debug("Context cancelled during health check; closing candidate to unblock goroutine")
				candidate.conn.Close() // interrupt blocking health-check I/O
				<-healthResult         // drain — goroutine exits quickly now
				p.mu.Lock()
				p.removeConnLocked(remoteAddr, candidate.conn)
				p.mu.Unlock()
				return nil
			}
		}

		if !healthy {
			// Health check failed. Close the connection and remove from pool.
			// Use removeConnLocked to keep connRegistry in sync (AUDIT M2 fix).
			if err := candidate.conn.Close(); err != nil {
				log.WithFields(logger.Fields{
					"pkg": "pool", "func": "ConnPool.GetWithContext",
					"remote_addr": candidate.remoteAddr,
				}).Warnf("failed to close unhealthy connection: %v", err)
			}
			p.mu.Lock()
			p.removeConnLocked(remoteAddr, candidate.conn)
			p.mu.Unlock()
			return nil
		}
		// Health check passed (or no health check configured)
		return &PoolConnWrapper{
			Conn: candidate.conn,
			pool: p,
			addr: remoteAddr,
		}
	}

	return nil
}

// getOrDialMu returns the per-address mutex for GetOrDial serialization.
// Returns an error if the dialMu map has been corrupted (contains non-mutex values).
func (p *ConnPool) getOrDialMu(remoteAddr string) (*sync.Mutex, error) {
	// Fast path: avoid allocating a new mutex on cache hits (AUDIT L-4).
	if v, ok := p.dialMu.Load(remoteAddr); ok {
		mu, ok := v.(*sync.Mutex)
		if !ok {
			return nil, oops.
				Code("INTERNAL_ERROR").
				In("pool").
				Errorf("dialMu corrupted for address %s: expected *sync.Mutex, got %T", remoteAddr, v)
		}
		return mu, nil
	}
	val, _ := p.dialMu.LoadOrStore(remoteAddr, &sync.Mutex{})
	mu, ok := val.(*sync.Mutex)
	if !ok {
		// AUDIT L6: This branch is unreachable in practice. dialMu is unexported
		// and all writes to it go through getOrDialMu itself via LoadOrStore, which
		// always stores *sync.Mutex values. The guard is retained as a defensive
		// invariant check; sync.Map is memory-safe and cannot corrupt stored types
		// through concurrent access.
		return nil, oops.
			Code("INTERNAL_ERROR").
			In("pool").
			Errorf("dialMu corrupted for address %s: expected *sync.Mutex, got %T", remoteAddr, val)
	}
	return mu, nil
}

// GetOrDial atomically retrieves an idle connection for remoteAddr or, if none
// is available, calls dial to create a new one. The dial function is called
// outside the pool lock so it may perform blocking I/O (e.g., TCP connect +
// Noise handshake), but only one goroutine at a time will dial for a given
// remoteAddr. This prevents the TOCTOU race where multiple goroutines
// simultaneously discover an empty pool and each dial a fresh connection to
// the same NTCP2 router — which the NTCP2 spec considers a protocol error
// (§2.1: "only one active NTCP2 session per router").
//
// The returned connection is wrapped in a PoolConnWrapper. If dial succeeds,
// the new connection is added to the pool and checked out in a single
// atomic step.
//
// If ctx is cancelled before dial completes, GetOrDial returns ctx.Err().
//
// LOCKING AND HEALTH CHECK: When the per-address dial lock (addrMu) is held
// and a re-check of the pool finds an existing connection, HealthCheck is
// executed while addrMu is still held. The lock is released immediately after
// Get() returns, so the HealthCheck itself runs within the lock window.
// Callers providing a HealthCheck that performs blocking I/O (e.g., a Noise
// ping round-trip) should be aware that concurrent GetOrDial calls for the
// same address will be serialised for the duration of each health check.
// For workloads with many concurrent callers per address and a slow
// HealthCheck, prefer GetWithContext() with a timeout to bound latency.
//
// ADDRESSING: This function pools connections under the user-provided remoteAddr
// parameter, allowing logical addressing (e.g., "proxy:8080"). This differs
// from Put(), which uses conn.RemoteAddr().String() for physical addressing.
// The returned PoolConnWrapper's Close() method uses Release() with the correct
// logical address, maintaining consistency. Users should not mix GetOrDial with
// manual Put() calls on the same connection unless the addresses match exactly.
func (p *ConnPool) GetOrDial(ctx context.Context, remoteAddr string, dial func(ctx context.Context) (net.Conn, error)) (PooledConnection, error) {
	// Fast path: try to get an existing connection.
	if conn := p.Get(remoteAddr); conn != nil {
		return conn, nil
	}

	// Serialize dialing per address to prevent duplicate sessions.
	addrMu, err := p.getOrDialMu(remoteAddr)
	if err != nil {
		return nil, err
	}
	addrMu.Lock()
	// AUDIT H1: Guard against panics in the dial callback permanently
	// deadlocking the per-address mutex. Track whether we have already
	// released the lock explicitly so the deferred function is a no-op
	// on the non-panic (normal) path.
	unlocked := false
	defer func() {
		if !unlocked {
			addrMu.Unlock()
		}
	}()

	// Re-check after acquiring the per-address lock — another goroutine
	// may have dialed and put a connection while we waited.
	if conn := p.Get(remoteAddr); conn != nil {
		addrMu.Unlock()
		unlocked = true
		return conn, nil
	}

	// Check context before dialing.
	if err := ctx.Err(); err != nil {
		addrMu.Unlock()
		unlocked = true
		return nil, err
	}

	// Check that the pool has not been closed since we acquired addrMu.
	// Without this guard, a concurrent Close() could allow a full dial
	// (including Noise handshake) to complete only to be discarded
	// inside putAndGet — wasting resources on both ends (AUDIT M-2).
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()
	if closed {
		addrMu.Unlock()
		unlocked = true
		return nil, oops.
			Code("POOL_CLOSED").
			In("pool").
			Errorf("GetOrDial: pool is closed")
	}

	// Dial outside the pool lock.
	log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.dialNew", "remote_addr": remoteAddr}).Debug("Dialing new connection for pool")
	conn, err := dial(ctx)

	// Validate dial callback return values (AUDIT M-4)
	if err != nil && conn != nil {
		addrMu.Unlock()
		unlocked = true
		conn.Close()
		return nil, oops.
			Code("INVALID_DIAL_RESULT").
			In("pool").
			Errorf("dial callback returned both connection and error; must return either (conn, nil) or (nil, error)")
	}

	if err == nil && conn == nil {
		addrMu.Unlock()
		unlocked = true
		return nil, oops.
			Code("INVALID_DIAL_RESULT").
			In("pool").
			Errorf("dial callback returned (nil, nil); must return either (conn, nil) or (nil, error)")
	}

	if err != nil {
		// Note: We intentionally do NOT delete dialMu[remoteAddr] here.
		// Cleanup would introduce a race where goroutine G1 loads mutex M1,
		// G2 deletes the entry, and G3 creates mutex M2 for the same address,
		// breaking the serialization guarantee (AUDIT H-1).
		addrMu.Unlock()
		unlocked = true
		return nil, oops.
			Code("DIAL_FAILED").
			In("pool").
			Wrapf(err, "GetOrDial: dial failed for %s", remoteAddr)
	}

	// Put the new connection into the pool and immediately check it out.
	// addrMu is held until putAndGet completes so that waiting goroutines
	// cannot observe an empty pool and dial a duplicate connection. This
	// closes the TOCTOU window documented in AUDIT H-1: the per-address
	// lock now covers the entire dial-and-insert operation atomically.
	wrapper, putErr := p.putAndGet(remoteAddr, conn)
	addrMu.Unlock()
	unlocked = true
	if putErr != nil {
		// Note: We intentionally do NOT delete dialMu[remoteAddr] here.
		// See comment above in dial error path (AUDIT H-1).
		return nil, putErr
	}
	return wrapper, nil
}

// putAndGet adds a newly-dialed connection to the pool and returns it
// as a checked-out PoolConnWrapper in a single atomic step.
func (p *ConnPool) putAndGet(remoteAddr string, conn net.Conn) (PooledConnection, error) {
	log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.putAndGet", "remote_addr": remoteAddr}).Debug("Adding and checking out connection")
	conn = unwrapPoolConn(conn)

	// Wrap ready check with panic recovery (AUDIT M-2)
	ready := true
	if p.readyCheck != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.WithFields(logger.Fields{
						"pkg": "pool", "func": "ConnPool.putAndGet",
						"remote_addr": remoteAddr,
					}).Errorf("ReadyCheck panicked: %v", r)
					ready = false
				}
			}()
			ready = p.readyCheck(conn)
		}()
	}

	if !ready {
		conn.Close()
		return nil, oops.
			Code("CONNECTION_NOT_READY").
			In("pool").
			Errorf("GetOrDial: connection failed ReadyCheck for %s", remoteAddr)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		conn.Close()
		return nil, oops.
			Code("POOL_CLOSED").
			In("pool").
			Errorf("GetOrDial: pool is closed")
	}

	connList := p.conns[remoteAddr]
	if p.exceedsCapacity(connList) {
		conn.Close()
		return nil, oops.
			Code("POOL_FULL").
			In("pool").
			Errorf("GetOrDial: pool at capacity for %s", remoteAddr)
	}

	pc := newPooledConn(conn, remoteAddr)
	pc.inUse = true
	p.conns[remoteAddr] = append(connList, pc)
	p.connRegistry[conn] = remoteAddr // Register connection address mapping (AUDIT H-2)

	return &PoolConnWrapper{
		Conn: conn,
		pool: p,
		addr: remoteAddr,
	}, nil
}

// Put adds a connection to the pool for reuse.
//
// Callers must only Put() connections whose Noise handshake has been
// completed. If a ReadyCheck callback is configured in PoolConfig, it is
// called before pooling; the connection is rejected (closed) if the check
// returns false. Without a ReadyCheck, it is the caller's responsibility
// to ensure the connection is in a usable state.
//
// ADDRESSING (AUDIT L-1): Put() uses conn.RemoteAddr().String() as the pool
// key, which reflects the connection's physical address. This differs from
// GetOrDial(), which pools under the user-provided logical address parameter.
// WARNING: If you obtained the connection via GetOrDial with a logical address
// (e.g., "proxy:8080"), DO NOT call Put() manually — use the wrapper's Close()
// method instead, or the connection will be pooled under its physical address
// (e.g., "10.0.0.1:8080") and Get("proxy:8080") will fail to find it.
//
// PERFORMANCE (AUDIT L-2): The RemoteAddr().String() method is called before
// acquiring the pool lock. If your net.Addr implementation's String() method
// is slow or blocking, this will delay connection pooling. Ensure RemoteAddr()
// returns quickly and returns a non-empty string.
func (p *ConnPool) Put(conn net.Conn) error {
	if conn == nil {
		log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.Put"}).Warn("Attempted to put nil connection in pool")
		return oops.
			Code("INVALID_CONNECTION").
			In("pool").
			Errorf("cannot put nil connection in pool")
	}

	conn = unwrapPoolConn(conn)

	// Wrap ready check with panic recovery (AUDIT M-2)
	ready := true
	if p.readyCheck != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.WithFields(logger.Fields{
						"pkg": "pool", "func": "ConnPool.Put",
					}).Errorf("ReadyCheck panicked: %v", r)
					ready = false
				}
			}()
			ready = p.readyCheck(conn)
		}()
	}

	if !ready {
		return oops.
			Code("CONNECTION_NOT_READY").
			In("pool").
			Errorf("connection failed ReadyCheck; not pooled")
	}

	addr := conn.RemoteAddr()
	if addr == nil {
		return oops.
			Code("INVALID_CONNECTION").
			In("pool").
			Errorf("connection has nil RemoteAddr")
	}
	remoteAddr := addr.String()

	// Validate non-empty address (AUDIT L-2)
	if remoteAddr == "" {
		return oops.
			Code("INVALID_CONNECTION").
			In("pool").
			Errorf("connection has empty RemoteAddr")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		conn.Close()
		return oops.
			Code("POOL_CLOSED").
			In("pool").
			Errorf("pool is closed; connection not pooled")
	}

	connList := p.conns[remoteAddr]

	if p.exceedsCapacity(connList) {
		conn.Close()
		return oops.
			Code("POOL_FULL").
			In("pool").
			Errorf("pool at capacity; connection not pooled")
	}

	if p.isDuplicateConn(connList, conn) {
		// Return a distinct error so callers can detect accidental double-puts
		// rather than silently succeeding (AUDIT L7 fix). The connection is
		// NOT closed; the caller still owns it.
		log.WithFields(logger.Fields{
			"pkg": "pool", "func": "ConnPool.Put", "remote_addr": remoteAddr,
		}).Warn("connection already pooled; Put is a no-op")
		return oops.Code("CONNECTION_ALREADY_POOLED").In("pool").
			Errorf("connection already pooled for address %s", remoteAddr)
	}

	p.conns[remoteAddr] = append(connList, newPooledConn(conn, remoteAddr))
	p.connRegistry[conn] = remoteAddr // Register connection address mapping (AUDIT H-2)
	return nil
}

// unwrapPoolConn extracts the underlying net.Conn from a PoolConnWrapper to avoid
// wrapper-inside-wrapper nesting.
func unwrapPoolConn(conn net.Conn) net.Conn {
	if wrapper, ok := conn.(*PoolConnWrapper); ok {
		return wrapper.Conn
	}
	return conn
}

// exceedsCapacity returns true if the pool has no room for a new connection,
// considering both per-address and global limits.
// A maxSize of 0 is treated as "no per-address limit" to avoid silently
// closing every connection when the caller explicitly sets MaxSize to zero.
func (p *ConnPool) exceedsCapacity(connList []*PooledConn) bool {
	if p.maxSize > 0 && len(connList) >= p.maxSize {
		return true
	}
	if p.maxTotal > 0 && p.totalConnsLocked() >= p.maxTotal {
		return true
	}
	return false
}

// isDuplicateConn checks whether the connection is already present in the pool
// under any address (AUDIT H-2). Returns true if the connection is already pooled.
func (p *ConnPool) isDuplicateConn(connList []*PooledConn, conn net.Conn) bool {
	// Check the global registry (AUDIT L-6: linear scan removed because
	// connRegistry is kept in sync with connList in all Put/Remove operations)
	_, found := p.connRegistry[conn]
	return found
}

// newPooledConn creates a new PooledConn entry with current timestamps.
func newPooledConn(conn net.Conn, remoteAddr string) *PooledConn {
	return &PooledConn{
		conn:       conn,
		created:    time.Now(),
		lastUsed:   time.Now(),
		inUse:      false,
		remoteAddr: remoteAddr,
	}
}

// Release marks a connection as no longer in use, making it available for reuse.
// Returns an error if the pool is closed or the connection is not found.
//
// When the pool is closed, Release closes the connection and returns a
// POOL_CLOSED-coded error regardless of whether the close succeeded or failed.
// This lets callers (e.g., PoolConnWrapper.Close) distinguish "pool was closed,
// connection already closed inside Release" from other release failures that
// require the caller to close the connection themselves (L-4, L-6).
//
// If conn is a *PoolConnWrapper, the wrapper is marked closed so that a
// subsequent call to wrapper.Close() returns an ALREADY_CLOSED error instead
// of issuing a second release (preventing a double-release vulnerability).
func (p *ConnPool) Release(remoteAddr string, conn net.Conn) error {
	// Explicit nil check for better error messages (AUDIT L-1)
	if conn == nil {
		return oops.Code("INVALID_CONNECTION").In("pool").
			Errorf("cannot release nil connection")
	}

	// Unwrap PoolConnWrapper for correct identity comparison.
	// Mark the wrapper closed to prevent a subsequent Close() from
	// issuing a second release.
	if wrapper, ok := conn.(*PoolConnWrapper); ok {
		wrapper.mu.Lock()
		wrapper.closed = true
		wrapper.mu.Unlock()
		conn = wrapper.Conn
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		// Remove the in-use entry from the map so Stats()/Drain() no
		// longer see it, then close the underlying connection best-effort.
		// POOL_CLOSED is always returned so callers (e.g. PoolConnWrapper)
		// can distinguish "pool closed, conn already closed" from other
		// failures and avoid calling Close() a second time (L-4, L-6).
		p.removeConnLocked(remoteAddr, conn)
		conn.Close() //nolint:errcheck // best-effort; POOL_CLOSED is the primary signal
		return oops.Code("POOL_CLOSED").In("pool").
			Errorf("pool closed; connection closed during release for %s", remoteAddr)
	}

	connList, exists := p.conns[remoteAddr]
	if !exists {
		return oops.Code("CONNECTION_NOT_FOUND").In("pool").
			Errorf("connection not found for address %s", remoteAddr)
	}

	for _, pooledConn := range connList {
		if pooledConn.conn == conn {
			pooledConn.inUse = false
			pooledConn.lastUsed = time.Now()
			return nil
		}
	}

	return oops.Code("CONNECTION_NOT_FOUND").In("pool").
		Errorf("connection not found in pool for address %s", remoteAddr)
}

// removeConnLocked removes a specific connection from the pool's internal map.
// Must be called with p.mu held.
func (p *ConnPool) removeConnLocked(remoteAddr string, conn net.Conn) {
	connList, exists := p.conns[remoteAddr]
	if !exists {
		// AUDIT L-7: Defensive cleanup even if address not found
		delete(p.connRegistry, conn)
		return
	}
	for i, pooledConn := range connList {
		if pooledConn.conn == conn {
			connList = append(connList[:i], connList[i+1:]...)
			p.updateConnectionMap(remoteAddr, connList)
			delete(p.connRegistry, conn) // Clean up registry (AUDIT H-2)
			return
		}
	}
	// AUDIT L-7: Defensive cleanup even if connection not in list
	// (should never happen if invariants are maintained, but defends
	// against future bugs that break registry/connList sync)
	delete(p.connRegistry, conn)
}

// Remove closes a connection and permanently removes it from the pool.
// Use this when a connection is known to be broken.
//
// Returns CONNECTION_NOT_FOUND if the connection was not in the pool
// for the given address (the connection is still closed in this case
// to avoid resource leaks). Returns nil on success.
func (p *ConnPool) Remove(remoteAddr string, conn net.Conn) error {
	log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.Remove", "remote_addr": remoteAddr}).Debug("Removing connection from pool")

	// Explicit nil check for better error messages (AUDIT L-1)
	if conn == nil {
		return oops.Code("INVALID_CONNECTION").In("pool").
			Errorf("cannot remove nil connection")
	}

	if wrapper, ok := conn.(*PoolConnWrapper); ok {
		// Mark the wrapper closed to prevent a subsequent Close() from
		// issuing a second Remove/Release call (AUDIT L-2).
		wrapper.mu.Lock()
		wrapper.closed = true
		wrapper.mu.Unlock()
		conn = wrapper.Conn
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Honour the closed state consistently with Get(), Put(), and Release().
	if p.closed {
		p.removeConnLocked(remoteAddr, conn)
		return conn.Close()
	}

	connList, exists := p.conns[remoteAddr]
	if !exists {
		// Close the connection to prevent resource leaks, but signal
		// to the caller that it was not found in the pool.
		conn.Close()
		return oops.Code("CONNECTION_NOT_FOUND").In("pool").
			Errorf("connection not found for address %s (closed anyway)", remoteAddr)
	}

	for _, pooledConn := range connList {
		if pooledConn.conn == conn {
			// Use removeConnLocked to keep connRegistry in sync (AUDIT M3 fix).
			p.removeConnLocked(remoteAddr, conn)
			return conn.Close()
		}
	}

	// Connection not found in the list for this address.
	conn.Close()
	return oops.Code("CONNECTION_NOT_FOUND").In("pool").
		Errorf("connection not in pool list for address %s (closed anyway)", remoteAddr)
}
