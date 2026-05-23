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

// DefaultDrainPollInterval is the default polling interval for Drain when
// waiting for in-use connections to be released.
//
// RATIONALE (AUDIT L-3): 50ms balances responsiveness (drain completes within
// ~100ms of the last connection being released) against CPU overhead (20 checks
// per second per waiting Drain call). Shorter intervals (e.g., 10ms) would
// provide faster drain at the cost of 100 checks/second CPU load; longer
// intervals (e.g., 100ms) would reduce overhead but delay drain completion.
const DefaultDrainPollInterval = 50 * time.Millisecond

// MinCleanupInterval is the minimum interval between background cleanup runs
// to prevent tight loops when MaxIdle/MaxAge are very small.
//
// RATIONALE (AUDIT L-3): 1 second prevents excessive cleanup overhead when
// pool is configured with very short timeouts (e.g., MaxIdle=2s for testing).
// Even with 2-second timeouts, checking every second ensures expired connections
// are removed within 1 second of expiration, which is acceptable latency. Values
// below 1s (e.g., 100ms) would cause unnecessary CPU churn for little benefit.
const MinCleanupInterval = time.Second

// DefaultCleanupInterval is the default interval between background cleanup
// runs when MaxIdle and MaxAge are not set or are large.
//
// RATIONALE (AUDIT L-3): 1 minute provides a reasonable default for long-lived
// pools where connections may be idle for hours. More frequent cleanup (e.g.,
// every 10s) provides minimal benefit for typical pool lifetimes and increases
// lock contention. Less frequent cleanup (e.g., 5 minutes) would delay removal
// of expired connections too long for interactive workloads.
const DefaultCleanupInterval = time.Minute

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
	//
	// PANIC RECOVERY (AUDIT M-3): If the callback panics, the panic is
	// recovered and logged, and the connection is treated as unhealthy
	// (removed from pool and closed). The panic does not propagate to
	// the caller of Get(). Callers cannot distinguish between a panic
	// and a legitimate "false" return value.
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
	//
	// PANIC RECOVERY (AUDIT M-3): If the callback panics, the panic is
	// recovered and logged, and the connection is treated as not ready
	// (rejected from pool and closed). The panic does not propagate to
	// the caller of Put().
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
	// Release marks a connection as available for reuse without closing it.
	Release(addr string, conn net.Conn) error
	// Remove removes a connection from the pool and closes it.
	Remove(addr string, conn net.Conn) error
	// Drain waits for all in-use connections to be returned, up to the context deadline.
	Drain(ctx context.Context) error
	// Snapshot returns a shallow copy of the current pool state for inspection.
	Snapshot() []*PooledConn
	// Stats returns pool statistics (total connections, in-use count, etc.).
	Stats() map[string]int
	// Close closes the pool and all connections it holds.
	Close() error
}

// ConnPool manages a pool of reusable connections for performance optimization.
// It only uses interface types (net.Conn, net.Addr) for maximum compatibility.
type ConnPool struct {
	mu           sync.RWMutex
	conns        map[string][]*PooledConn // keyed by remote address
	connRegistry map[net.Conn]string      // tracks which address each conn is pooled under (AUDIT H-2)
	maxSize      int
	maxTotal     int
	maxAge       time.Duration
	maxIdle      time.Duration
	healthCheck  func(net.Conn) bool
	readyCheck   func(net.Conn) bool
	closed       bool
	done         chan struct{}
	// dialMu serializes GetOrDial per address to prevent TOCTOU races.
	dialMu sync.Map // map[string]*sync.Mutex
	// cleanupWg tracks the cleanup goroutine for proper shutdown (AUDIT M-2 fix)
	cleanupWg sync.WaitGroup
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
		// AUDIT L-5: Warn on negative values (semantically invalid)
		if config.MaxSize < 0 {
			log.WithFields(logger.Fields{
				"pkg": "pool", "func": "NewConnPool",
			}).Warnf("Negative MaxSize (%d) is invalid; applying default %d", config.MaxSize, DefaultMaxSize)
		}
		config.MaxSize = DefaultMaxSize
	}

	pool := &ConnPool{
		conns:        make(map[string][]*PooledConn),
		connRegistry: make(map[net.Conn]string),
		maxSize:      config.MaxSize,
		maxTotal:     config.MaxTotal,
		maxAge:       config.MaxAge,
		maxIdle:      config.MaxIdle,
		healthCheck:  config.HealthCheck,
		readyCheck:   config.ReadyCheck,
		done:         make(chan struct{}),
	}

	// Start cleanup goroutine (AUDIT M-2 fix: track with WaitGroup)
	pool.cleanupWg.Add(1)
	go pool.cleanup()

	return pool
}

// Get retrieves a connection from the pool for the given remote address.
// Returns nil if no suitable connection is available.
func (p *ConnPool) Get(remoteAddr string) net.Conn {
	var candidate *PooledConn
	var toClose []*PooledConn

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.Get"}).Debug("Get called on closed pool")
		return nil
	}

	connList, exists := p.conns[remoteAddr]
	if !exists || len(connList) == 0 {
		p.mu.Unlock()
		return nil
	}

	// Find an available and valid connection.
	// Expired connections are collected for closing outside the lock.
	for i := 0; i < len(connList); i++ {
		pooledConn := connList[i]
		if pooledConn.inUse {
			continue
		}
		if !p.isValid(pooledConn) {
			// Collect for closing outside the lock
			toClose = append(toClose, pooledConn)
			connList = append(connList[:i], connList[i+1:]...)
			i--
			p.updateConnectionMap(remoteAddr, connList)
			continue
		}
		// Found a valid candidate. Mark as tentatively in-use and break.
		candidate = pooledConn
		candidate.inUse = true
		candidate.lastUsed = time.Now()
		break
	}
	p.mu.Unlock()

	// Close expired connections outside the lock
	for _, pc := range toClose {
		if err := pc.conn.Close(); err != nil {
			log.WithFields(logger.Fields{
				"pkg": "pool", "func": "ConnPool.Get",
				"remote_addr": pc.remoteAddr,
			}).Warnf("failed to close expired connection: %v", err)
		}
	}

	// Run health check outside the lock if we have a candidate
	if candidate != nil {
		// Wrap health check with panic recovery (AUDIT M-2)
		healthy := true
		if p.healthCheck != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.WithFields(logger.Fields{
							"pkg": "pool", "func": "ConnPool.Get",
							"remote_addr": candidate.remoteAddr,
						}).Errorf("HealthCheck panicked: %v", r)
						healthy = false
					}
				}()
				healthy = p.healthCheck(candidate.conn)
			}()
		}

		if !healthy {
			// Health check failed. Re-acquire lock, mark as not in-use, and remove.
			if err := candidate.conn.Close(); err != nil {
				log.WithFields(logger.Fields{
					"pkg": "pool", "func": "ConnPool.Get",
					"remote_addr": candidate.remoteAddr,
				}).Warnf("failed to close unhealthy connection: %v", err)
			}
			p.mu.Lock()
			connList := p.conns[remoteAddr]
			for i, pc := range connList {
				if pc == candidate {
					connList = append(connList[:i], connList[i+1:]...)
					p.updateConnectionMap(remoteAddr, connList)
					break
				}
			}
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
func (p *ConnPool) GetWithContext(ctx context.Context, remoteAddr string) net.Conn {
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

	// Find an available and valid connection.
	// Expired connections are collected for closing outside the lock.
	for i := 0; i < len(connList); i++ {
		pooledConn := connList[i]
		if pooledConn.inUse {
			continue
		}
		if !p.isValid(pooledConn) {
			// Collect for closing outside the lock
			toClose = append(toClose, pooledConn)
			connList = append(connList[:i], connList[i+1:]...)
			i--
			p.updateConnectionMap(remoteAddr, connList)
			continue
		}
		// Found a valid candidate. Mark as tentatively in-use and break.
		candidate = pooledConn
		candidate.inUse = true
		candidate.lastUsed = time.Now()
		break
	}
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
				// Context cancelled during health check
				log.WithFields(logger.Fields{
					"pkg": "pool", "func": "ConnPool.GetWithContext",
					"remote_addr": candidate.remoteAddr,
				}).Debug("Context cancelled during health check")
				// Return connection to pool
				p.mu.Lock()
				candidate.inUse = false
				p.mu.Unlock()
				return nil
			}
		}

		if !healthy {
			// Health check failed. Re-acquire lock, mark as not in-use, and remove.
			if err := candidate.conn.Close(); err != nil {
				log.WithFields(logger.Fields{
					"pkg": "pool", "func": "ConnPool.GetWithContext",
					"remote_addr": candidate.remoteAddr,
				}).Warnf("failed to close unhealthy connection: %v", err)
			}
			p.mu.Lock()
			connList := p.conns[remoteAddr]
			for i, pc := range connList {
				if pc == candidate {
					connList = append(connList[:i], connList[i+1:]...)
					p.updateConnectionMap(remoteAddr, connList)
					break
				}
			}
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
	val, _ := p.dialMu.LoadOrStore(remoteAddr, &sync.Mutex{})
	mu, ok := val.(*sync.Mutex)
	if !ok {
		// AUDIT H-2 fix: return error instead of panic to prevent process crash
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
// ADDRESSING: This function pools connections under the user-provided remoteAddr
// parameter, allowing logical addressing (e.g., "proxy:8080"). This differs
// from Put(), which uses conn.RemoteAddr().String() for physical addressing.
// The returned PoolConnWrapper's Close() method uses Release() with the correct
// logical address, maintaining consistency. Users should not mix GetOrDial with
// manual Put() calls on the same connection unless the addresses match exactly.
func (p *ConnPool) GetOrDial(ctx context.Context, remoteAddr string, dial func(ctx context.Context) (net.Conn, error)) (net.Conn, error) {
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

	// Re-check after acquiring the per-address lock — another goroutine
	// may have dialed and put a connection while we waited.
	if conn := p.Get(remoteAddr); conn != nil {
		addrMu.Unlock()
		return conn, nil
	}

	// Check context before dialing.
	if err := ctx.Err(); err != nil {
		addrMu.Unlock()
		return nil, err
	}

	// Dial outside the pool lock.
	log.WithFields(logger.Fields{"pkg": "pool", "func": "ConnPool.dialNew", "remote_addr": remoteAddr}).Debug("Dialing new connection for pool")
	conn, err := dial(ctx)

	// Release addrMu immediately after dial completes, before ReadyCheck.
	// This allows other goroutines to proceed without waiting for our
	// potentially-slow ReadyCheck callback.
	addrMu.Unlock()

	// Validate dial callback return values (AUDIT M-4)
	if err != nil && conn != nil {
		conn.Close()
		return nil, oops.
			Code("INVALID_DIAL_RESULT").
			In("pool").
			Errorf("dial callback returned both connection and error; must return either (conn, nil) or (nil, error)")
	}

	if err == nil && conn == nil {
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
		return nil, oops.
			Code("DIAL_FAILED").
			In("pool").
			Wrapf(err, "GetOrDial: dial failed for %s", remoteAddr)
	}

	// Put the new connection into the pool and immediately check it out.
	wrapper, putErr := p.putAndGet(remoteAddr, conn)
	if putErr != nil {
		// Note: We intentionally do NOT delete dialMu[remoteAddr] here.
		// See comment above in dial error path (AUDIT H-1).
		return nil, putErr
	}
	return wrapper, nil
}

// putAndGet adds a newly-dialed connection to the pool and returns it
// as a checked-out PoolConnWrapper in a single atomic step.
func (p *ConnPool) putAndGet(remoteAddr string, conn net.Conn) (net.Conn, error) {
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
		return nil
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
	ticker := time.NewTicker(DefaultDrainPollInterval)
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
				Wrapf(ctx.Err(), "drain: %d connections still in use", stats["in_use"])
		case <-ticker.C:
			// Poll again.
		}
	}
}

// Snapshot returns a point-in-time copy of all pooled connections' metadata.
// Each returned PooledConn is a shallow copy — the underlying net.Conn is
// shared with the pool, so callers must not Close or Write on it. Use
// Snapshot returns a shallow copy of all pooled connections for diagnostics,
// monitoring, or testing purposes.
//
// WARNING (AUDIT L-4): The returned PooledConn structs share the underlying
// net.Conn references with the active pool. DO NOT call Close(), Write(), or
// Read() on conn.NetConn() — doing so will corrupt pool state, cause data
// races (detectable with -race), and crash concurrent users. Treat returned
// PooledConn entries as read-only metadata snapshots. If you need to interact
// with connections, use Get()/GetOrDial() instead. Use Snapshot() only for
// read-only inspection of metadata (created, lastUsed, inUse, remoteAddr).
//
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
			p.dialMu.Delete(addr)
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

// Stats returns a snapshot of current pool statistics. The returned map is
// a fresh allocation and can be safely modified by the caller without affecting
// pool state (AUDIT M-6). Call Stats() again to get updated values.
//
// The returned map contains:
//   - "total": total number of connections (in-use + available)
//   - "in_use": connections currently checked out
//   - "available": connections ready to be checked out
//   - "addresses": number of unique remote addresses
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
	interval := DefaultCleanupInterval
	if p.maxIdle > 0 && p.maxIdle/2 < interval {
		interval = p.maxIdle / 2
	}
	if p.maxAge > 0 && p.maxAge/2 < interval {
		interval = p.maxAge / 2
	}
	if interval < MinCleanupInterval {
		interval = MinCleanupInterval
	}
	// AUDIT M-5 fix: defend against zero/negative intervals
	if interval <= 0 {
		interval = DefaultCleanupInterval
	}
	return interval
}

// cleanup runs periodically to remove expired connections
func (p *ConnPool) cleanup() {
	defer p.cleanupWg.Done() // AUDIT M-2 fix: signal cleanup goroutine exit
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
	var toClose []*PooledConn

	p.mu.Lock()
	for addr, connList := range p.conns {
		validConns, expired := p.filterValidConnections(connList)
		toClose = append(toClose, expired...)
		p.updateConnectionMap(addr, validConns)
	}

	// Clean up orphaned dialMu entries (AUDIT H-1 fix).
	// If an address has no pooled connections, its dial mutex can be removed.
	// This prevents unbounded memory growth in long-running processes that
	// dial many transient addresses (e.g., failed dials, expired connections).
	p.dialMu.Range(func(key, value interface{}) bool {
		addr, ok := key.(string)
		if !ok {
			return true // skip malformed key
		}
		if _, exists := p.conns[addr]; !exists {
			p.dialMu.Delete(addr)
		}
		return true
	})
	p.mu.Unlock()

	// Close expired connections outside the lock
	for _, pc := range toClose {
		pc.conn.Close()
	}
}

// filterValidConnections separates valid connections from expired ones.
// Returns two slices: valid connections and expired connections to close.
func (p *ConnPool) filterValidConnections(connList []*PooledConn) ([]*PooledConn, []*PooledConn) {
	validConns := make([]*PooledConn, 0, len(connList))
	expiredConns := make([]*PooledConn, 0)

	for _, pooledConn := range connList {
		if p.shouldKeepConnection(pooledConn) {
			validConns = append(validConns, pooledConn)
		} else {
			expiredConns = append(expiredConns, pooledConn)
		}
	}

	return validConns, expiredConns
}

// shouldKeepConnection determines if a connection should be retained
func (p *ConnPool) shouldKeepConnection(pooledConn *PooledConn) bool {
	return pooledConn.inUse || p.isValid(pooledConn)
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
