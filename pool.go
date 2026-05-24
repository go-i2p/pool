// Package pool provides a connection pool for reusing Noise-encrypted connections
// across multiple Dial operations, reducing handshake overhead for repeated peers.
package pool

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/go-i2p/logger"
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

// PoolStats holds a snapshot of pool statistics returned by Stats().
// All fields are safe to read without holding any lock.
type PoolStats struct {
	// Total is the total number of connections (in-use + available).
	Total int
	// InUse is the number of connections currently checked out.
	InUse int
	// Available is the number of connections ready to be checked out.
	Available int
	// Addresses is the number of unique remote addresses in the pool.
	Addresses int
}

// Pool is the interface satisfied by *ConnPool.
// Callers that need to substitute a test double, an LRU pool, or a
// metrics-instrumented pool can depend on Pool instead of *ConnPool.
type Pool interface {
	// Get retrieves an idle connection for remoteAddr, or nil if none is available.
	Get(remoteAddr string) PooledConnection
	// GetWithContext retrieves an idle connection for remoteAddr, respecting
	// context cancellation during health check execution. This method is
	// recommended over Get() for applications with request deadlines or
	// timeout requirements (AUDIT L1 fix).
	GetWithContext(ctx context.Context, remoteAddr string) PooledConnection
	// Put returns a connection to the pool for reuse.
	Put(conn net.Conn) error
	// GetOrDial retrieves an idle connection for remoteAddr or dials a new one.
	GetOrDial(ctx context.Context, remoteAddr string, dial func(context.Context) (net.Conn, error)) (PooledConnection, error)
	// Drain waits for all in-use connections to be returned, up to the context deadline.
	Drain(ctx context.Context) error
	// Snapshot returns a read-only metadata snapshot of the current pool state.
	// The returned ConnSnapshot values carry no live net.Conn handles and are
	// safe to store and inspect without risk of corrupting pool state.
	Snapshot() []ConnSnapshot
	// Stats returns pool statistics (total connections, in-use count, etc.).
	Stats() PoolStats
	// Close closes the pool and all connections it holds.
	Close() error
}

// Compile-time assertion: *ConnPool must satisfy the Pool interface (AUDIT L-1).
// If a future change breaks this contract, the build fails immediately here.
var _ Pool = (*ConnPool)(nil)

// ConnPool manages a pool of reusable connections for performance optimization.
// It only uses interface types (net.Conn, net.Addr) for maximum compatibility.
//
// Memory growth note: dialMu retains one *sync.Mutex per unique remote address
// ever passed to GetOrDial. These entries are never deleted because deletion
// would reintroduce the TOCTOU race that dialMu is designed to prevent. For a
// long-running I2P router that contacts many unique peers, expect roughly
// 24–48 bytes of retained heap per unique address dialed over the pool's
// lifetime. At 10,000 unique peers this is approximately 240–480 KB; at
// 100,000 peers it is approximately 2.4–4.8 MB. If per-address mutex retention
// is unacceptable for your workload, create a new ConnPool periodically and
// migrate callers to the new instance (AUDIT M-4).
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

// NewConnPool creates a new connection pool with the given configuration.
// The caller's *PoolConfig is read but never modified; effective defaults are
// derived into local variables only (M-1).
func NewConnPool(config *PoolConfig) *ConnPool {
	log.WithFields(logger.Fields{"pkg": "pool", "func": "NewConnPool"}).Debug("Creating new connection pool")
	if config == nil {
		config = &PoolConfig{
			MaxSize: DefaultMaxSize,
			MaxAge:  30 * time.Minute,
			MaxIdle: 5 * time.Minute,
		}
	}
	// Derive effective MaxSize without mutating the caller's struct (M-1).
	// Per the Go standard library convention (http.Server, tls.Config), config
	// structs are read but never modified by the function that consumes them.
	effectiveMaxSize := config.MaxSize
	if effectiveMaxSize <= 0 && !config.Unbounded {
		// Warn on negative values (semantically invalid)
		if effectiveMaxSize < 0 {
			log.WithFields(logger.Fields{
				"pkg": "pool", "func": "NewConnPool",
			}).Warnf("Negative MaxSize (%d) is invalid; applying default %d", effectiveMaxSize, DefaultMaxSize)
		}
		effectiveMaxSize = DefaultMaxSize
	}

	pool := &ConnPool{
		conns:        make(map[string][]*PooledConn),
		connRegistry: make(map[net.Conn]string),
		maxSize:      effectiveMaxSize,
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

// Config is an alias for PoolConfig following Go naming conventions.
// Prefer Config in new code; PoolConfig is retained for backward compatibility.
type Config = PoolConfig
