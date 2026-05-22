// Package pool provides a connection pool for reusing Noise-encrypted connections
// across multiple Dial operations, reducing handshake overhead for repeated peers.
package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/go-i2p/logger"
	"github.com/samber/oops"
)

// DefaultMaxSize is the default per-address connection limit applied when a
// caller supplies a PoolConfig without MaxSize and without setting Unbounded.
const DefaultMaxSize = 10

// PoolConfig configures a connection pool
type PoolConfig struct {
	// MaxSize is the maximum number of connections per remote address.
	// A zero or negative value is treated as "apply the default limit"
	// (see DefaultMaxSize). To deliberately disable the per-address limit
	// (NOT recommended — it is a file-descriptor exhaustion vector), set
	// Unbounded to true.
	MaxSize int
	// MaxTotal is the maximum total number of connections across all addresses.
	// A zero value means no global limit is enforced.
	MaxTotal int
	// MaxAge is the maximum age of a connection before it is closed.
	MaxAge time.Duration
	// MaxIdle is the maximum idle time before a connection is closed.
	MaxIdle time.Duration
	// Unbounded, when true, disables the safe default for MaxSize. Callers
	// must opt in explicitly to unbounded per-address pools so that a
	// forgotten MaxSize does not silently allow FD exhaustion (AUDIT L-1).
	Unbounded bool
	// HealthCheck is an optional callback to probe connection liveness
	// before returning it from Get(). Return true if healthy.
	HealthCheck func(net.Conn) bool
	// ReadyCheck is an optional callback invoked by Put() to verify that a
	// connection is ready for reuse (e.g., that a Noise handshake has been
	// completed). Return true if the connection is ready to be pooled.
	// When nil, all connections are accepted by Put().
	//
	// For NTCP2 connections, the recommended check is:
	//   func(c net.Conn) bool {
	//       if nc, ok := c.(*noise.NoiseConn); ok {
	//           return nc.GetConnectionState() == internal.StateEstablished
	//       }
	//       return true
	//   }
	ReadyCheck func(net.Conn) bool
}

// Pool is the interface satisfied by *ConnPool.
// Callers that need to substitute a test double, an LRU pool, or a
// metrics-instrumented pool can depend on Pool instead of *ConnPool.
type Pool interface {
	// Get retrieves an idle connection for remoteAddr, or nil if none is available.
	Get(remoteAddr string) net.Conn
	// Put returns a connection to the pool for reuse.
	Put(conn net.Conn) error
	// GetOrDial retrieves an idle connection for remoteAddr or dials a new one.
	GetOrDial(ctx context.Context, remoteAddr string, dial func(context.Context) (net.Conn, error)) (net.Conn, error)
	// Close closes the pool and all connections it holds.
	Close() error
}

// ConnPool manages a pool of reusable connections for performance optimization.
// It only uses interface types (net.Conn, net.Addr) for maximum compatibility.
type ConnPool struct {
	mu          sync.RWMutex
	conns       map[string][]*PooledConn // keyed by remote address
	maxSize     int
	maxTotal    int
	maxAge      time.Duration
	maxIdle     time.Duration
	healthCheck func(net.Conn) bool
	readyCheck  func(net.Conn) bool
	closed      bool
	done        chan struct{}
	// dialMu serializes GetOrDial per address to prevent TOCTOU races.
	dialMu sync.Map // map[string]*sync.Mutex
}

// NewConnPool creates a new connection pool with the given configuration
func NewConnPool(config *PoolConfig) *ConnPool {
	log.WithFields(logger.Fields{"pkg": "pool", "func": "NewConnPool"}).Debug("Creating new connection pool")
	if config == nil {
		config = &PoolConfig{
			MaxSize: DefaultMaxSize,
			MaxAge:  30 * time.Minute,
			MaxIdle: 5 * time.Minute,
		}
	}
	// Apply the default per-address limit unless the caller explicitly
	// opted in to unbounded growth (AUDIT L-1).
	if config.MaxSize <= 0 && !config.Unbounded {
		config.MaxSize = DefaultMaxSize
	}

	pool := &ConnPool{
		conns:       make(map[string][]*PooledConn),
		maxSize:     config.MaxSize,
		maxTotal:    config.MaxTotal,
		maxAge:      config.MaxAge,
		maxIdle:     config.MaxIdle,
		healthCheck: config.HealthCheck,
		readyCheck:  config.ReadyCheck,
		done:        make(chan struct{}),
	}

	// Start cleanup goroutine
	go pool.cleanup()

	return pool
}

// Get retrieves a connection from the pool for the given remote address.
// Returns nil if no suitable connection is available.
func (p *ConnPool) Get(remoteAddr string) net.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.Get"}).Debug("Get called on closed pool")
		return nil
	}

	connList, exists := p.conns[remoteAddr]
	if !exists || len(connList) == 0 {
		return nil
	}

	// Find an available, valid, and healthy connection.
	// Expired connections are closed and removed here to prevent them from
	// accumulating and inflating the count against maxSize (starvation fix).
	for i := 0; i < len(connList); i++ {
		pooledConn := connList[i]
		if pooledConn.inUse {
			continue
		}
		if !p.isValid(pooledConn) {
			// Remove expired connection immediately so it does not count against capacity.
			pooledConn.conn.Close()
			connList = append(connList[:i], connList[i+1:]...)
			i--
			p.updateConnectionMap(remoteAddr, connList)
			continue
		}
		if p.healthCheck != nil && !p.healthCheck(pooledConn.conn) {
			pooledConn.conn.Close()
			connList = append(connList[:i], connList[i+1:]...)
			i--
			p.updateConnectionMap(remoteAddr, connList)
			continue
		}
		pooledConn.inUse = true
		pooledConn.lastUsed = time.Now()
		return &PoolConnWrapper{
			Conn: pooledConn.conn,
			pool: p,
			addr: remoteAddr,
		}
	}

	return nil
}

// getOrDialMu returns the per-address mutex for GetOrDial serialization.
func (p *ConnPool) getOrDialMu(remoteAddr string) *sync.Mutex {
	val, _ := p.dialMu.LoadOrStore(remoteAddr, &sync.Mutex{})
	return val.(*sync.Mutex)
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
func (p *ConnPool) GetOrDial(ctx context.Context, remoteAddr string, dial func(ctx context.Context) (net.Conn, error)) (net.Conn, error) {
	// Fast path: try to get an existing connection.
	if conn := p.Get(remoteAddr); conn != nil {
		return conn, nil
	}

	// Serialize dialing per address to prevent duplicate sessions.
	addrMu := p.getOrDialMu(remoteAddr)
	addrMu.Lock()
	defer addrMu.Unlock()

	// Re-check after acquiring the per-address lock — another goroutine
	// may have dialed and put a connection while we waited.
	if conn := p.Get(remoteAddr); conn != nil {
		return conn, nil
	}

	// Check context before dialing.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Dial outside the pool lock.
	log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.dialNew", "remote_addr": remoteAddr}).Debug("Dialing new connection for pool")
	conn, err := dial(ctx)
	if err != nil {
		return nil, oops.
			Code("DIAL_FAILED").
			In("pool").
			Wrapf(err, "GetOrDial: dial failed for %s", remoteAddr)
	}

	// Put the new connection into the pool and immediately check it out.
	return p.putAndGet(remoteAddr, conn)
}

// putAndGet adds a newly-dialed connection to the pool and returns it
// as a checked-out PoolConnWrapper in a single atomic step.
func (p *ConnPool) putAndGet(remoteAddr string, conn net.Conn) (net.Conn, error) {
	log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.putAndGet", "remote_addr": remoteAddr}).Debug("Adding and checking out connection")
	conn = unwrapPoolConn(conn)

	if p.readyCheck != nil && !p.readyCheck(conn) {
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
func (p *ConnPool) Put(conn net.Conn) error {
	if conn == nil {
		log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.Put"}).Warn("Attempted to put nil connection in pool")
		return oops.
			Code("INVALID_CONNECTION").
			In("pool").
			Errorf("cannot put nil connection in pool")
	}

	conn = unwrapPoolConn(conn)

	if p.readyCheck != nil && !p.readyCheck(conn) {
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

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return conn.Close()
	}

	connList := p.conns[remoteAddr]

	if p.exceedsCapacity(connList) {
		return conn.Close()
	}

	if p.isDuplicateConn(connList, conn) {
		return nil
	}

	p.conns[remoteAddr] = append(connList, newPooledConn(conn, remoteAddr))
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

// isDuplicateConn checks whether the connection is already present in the pool.
func (p *ConnPool) isDuplicateConn(connList []*PooledConn, conn net.Conn) bool {
	for _, existing := range connList {
		if existing.conn == conn {
			return true
		}
	}
	return false
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
// If conn is a *PoolConnWrapper, the wrapper is marked closed so that a
// subsequent call to wrapper.Close() returns an ALREADY_CLOSED error instead
// of issuing a second release (preventing a double-release vulnerability).
func (p *ConnPool) Release(remoteAddr string, conn net.Conn) error {
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
		// longer see it, then close the underlying connection.
		p.removeConnLocked(remoteAddr, conn)
		return conn.Close()
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
		return
	}
	for i, pooledConn := range connList {
		if pooledConn.conn == conn {
			connList = append(connList[:i], connList[i+1:]...)
			p.updateConnectionMap(remoteAddr, connList)
			return
		}
	}
}

// Remove closes a connection and permanently removes it from the pool.
// Use this when a connection is known to be broken.
//
// Returns CONNECTION_NOT_FOUND if the connection was not in the pool
// for the given address (the connection is still closed in this case
// to avoid resource leaks). Returns nil on success.
func (p *ConnPool) Remove(remoteAddr string, conn net.Conn) error {
	log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.Remove", "remote_addr": remoteAddr}).Debug("Removing connection from pool")
	if wrapper, ok := conn.(*PoolConnWrapper); ok {
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

	for i, pooledConn := range connList {
		if pooledConn.conn == conn {
			connList = append(connList[:i], connList[i+1:]...)
			p.updateConnectionMap(remoteAddr, connList)
			return conn.Close()
		}
	}

	// Connection not found in the list for this address.
	conn.Close()
	return oops.Code("CONNECTION_NOT_FOUND").In("pool").
		Errorf("connection not in pool list for address %s (closed anyway)", remoteAddr)
}

// Drain waits for all in-use connections to be returned to the pool.
// It blocks until either all connections are idle (in_use == 0) or
// the provided context is cancelled. Use this during graceful shutdown
// to allow in-flight sessions to complete before calling Close().
//
// Drain does not prevent new connections from being checked out; it
// only waits for the current in-use count to reach zero. Callers
// should stop accepting new work before calling Drain.
func (p *ConnPool) Drain(ctx context.Context) error {
	const pollInterval = 50 * time.Millisecond
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		stats := p.Stats()
		if stats["in_use"] == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return oops.
				Code("DRAIN_TIMEOUT").
				In("pool").
				Wrapf(ctx.Err(), "drain: %d connections still in use", p.Stats()["in_use"])
		case <-ticker.C:
			// Poll again.
		}
	}
}

// Snapshot returns a point-in-time copy of all pooled connections' metadata.
// Each returned PooledConn is a shallow copy — the underlying net.Conn is
// shared with the pool, so callers must not Close or Write on it. Use
// Snapshot for diagnostics, monitoring, or testing where you need to inspect
// pool state without modifying it.
func (p *ConnPool) Snapshot() []*PooledConn {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result []*PooledConn
	for _, connList := range p.conns {
		for _, pc := range connList {
			result = append(result, &PooledConn{
				conn:       pc.conn,
				created:    pc.created,
				lastUsed:   pc.lastUsed,
				inUse:      pc.inUse,
				remoteAddr: pc.remoteAddr,
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
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.Close"}).Debug("Closing connection pool")
	p.closed = true
	close(p.done)

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
				if err := pooledConn.conn.Close(); err != nil {
					errs = append(errs, err)
				}
			}
		}
		if len(remaining) == 0 {
			delete(p.conns, addr)
			p.dialMu.Delete(addr)
		} else {
			p.conns[addr] = remaining
		}
	}

	return errors.Join(errs...)
}

// Stats returns pool statistics
func (p *ConnPool) Stats() map[string]int {
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

	return map[string]int{
		"total":     total,
		"in_use":    inUse,
		"available": total - inUse,
		"addresses": len(p.conns),
	}
}

// isValid checks if a pooled connection is still valid for use
func (p *ConnPool) isValid(pooledConn *PooledConn) bool {
	now := time.Now()

	// Check age limit
	if p.maxAge > 0 && now.Sub(pooledConn.created) > p.maxAge {
		return false
	}

	// Check idle time limit
	if p.maxIdle > 0 && now.Sub(pooledConn.lastUsed) > p.maxIdle {
		return false
	}

	return true
}

// totalConnsLocked returns the total connection count. Must hold mu.
func (p *ConnPool) totalConnsLocked() int {
	total := 0
	for _, list := range p.conns {
		total += len(list)
	}
	return total
}

// cleanupInterval returns the ticker period for the cleanup goroutine.
// It uses half the configured MaxIdle or MaxAge (whichever is smaller and
// non-zero) so that expired connections are evicted promptly for short-lived
// pool configurations rather than waiting the hardcoded 1-minute default.
func (p *ConnPool) cleanupInterval() time.Duration {
	const defaultInterval = time.Minute
	const minInterval = time.Second
	interval := defaultInterval
	if p.maxIdle > 0 && p.maxIdle/2 < interval {
		interval = p.maxIdle / 2
	}
	if p.maxAge > 0 && p.maxAge/2 < interval {
		interval = p.maxAge / 2
	}
	if interval < minInterval {
		interval = minInterval
	}
	return interval
}

// cleanup runs periodically to remove expired connections
func (p *ConnPool) cleanup() {
	ticker := time.NewTicker(p.cleanupInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if p.shouldStopCleanup() {
				return
			}
			p.performCleanupCycle()
		case <-p.done:
			return
		}
	}
}

// shouldStopCleanup checks if the cleanup process should be terminated.
// Uses RLock because it only reads the closed boolean.
func (p *ConnPool) shouldStopCleanup() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// performCleanupCycle executes a single cleanup cycle for all connections
func (p *ConnPool) performCleanupCycle() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, connList := range p.conns {
		validConns := p.filterValidConnections(connList)
		p.updateConnectionMap(addr, validConns)
	}
}

// filterValidConnections separates valid connections from expired ones
func (p *ConnPool) filterValidConnections(connList []*PooledConn) []*PooledConn {
	validConns := make([]*PooledConn, 0, len(connList))

	for _, pooledConn := range connList {
		if p.shouldKeepConnection(pooledConn) {
			validConns = append(validConns, pooledConn)
		} else {
			p.closeExpiredConnection(pooledConn)
		}
	}

	return validConns
}

// shouldKeepConnection determines if a connection should be retained
func (p *ConnPool) shouldKeepConnection(pooledConn *PooledConn) bool {
	return pooledConn.inUse || p.isValid(pooledConn)
}

// closeExpiredConnection properly closes an expired connection
func (p *ConnPool) closeExpiredConnection(pooledConn *PooledConn) {
	pooledConn.conn.Close()
}

// updateConnectionMap updates the pool map with valid connections.
// When the last connection for an address is removed, the corresponding
// per-address dial mutex is also deleted from dialMu to prevent unbounded
// memory growth in long-running processes (NOTE: dialMu cleanup).
func (p *ConnPool) updateConnectionMap(addr string, validConns []*PooledConn) {
	if len(validConns) == 0 {
		delete(p.conns, addr)
		p.dialMu.Delete(addr)
	} else {
		p.conns[addr] = validConns
	}
}
