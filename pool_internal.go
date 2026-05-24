package pool

import (
	"time"

	"github.com/go-i2p/logger"
)

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

// findCandidate selects the first idle, valid connection from connList and
// returns it (marked inUse) along with any expired connections that should be
// closed outside the lock. The caller must hold p.mu.
func (p *ConnPool) findCandidate(remoteAddr string, connList []*PooledConn) (candidate *PooledConn, toClose []*PooledConn) {
	for i := 0; i < len(connList); i++ {
		pooledConn := connList[i]
		if pooledConn.inUse {
			continue
		}
		if !p.isValid(pooledConn) {
			toClose = append(toClose, pooledConn)
			connList = append(connList[:i], connList[i+1:]...)
			i--
			delete(p.connRegistry, pooledConn.conn) // keep registry in sync (AUDIT M-1)
			p.updateConnectionMap(remoteAddr, connList)
			continue
		}
		candidate = pooledConn
		candidate.inUse = true
		candidate.lastUsed = time.Now()
		break
	}
	return candidate, toClose
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

	// L3 fix: skip ticker entirely when no expiry limits are configured;
	// there is nothing for the cleanup cycle to do, so just wait for shutdown.
	if p.maxIdle == 0 && p.maxAge == 0 {
		<-p.done
		return
	}

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

// closeExpired closes this expired pooled connection, logging any error.
func (pc *PooledConn) closeExpired() {
	if err := pc.conn.Close(); err != nil {
		log.WithFields(logger.Fields{
			"pkg": "pool", "func": "performCleanupCycle",
			"remote_addr": pc.remoteAddr,
		}).Warnf("failed to close expired connection: %v", err)
	}
}

// performCleanupCycle executes a single cleanup cycle for all connections
func (p *ConnPool) performCleanupCycle() {
	var toClose []*PooledConn

	p.mu.Lock()
	for addr, connList := range p.conns {
		validConns, expired := p.filterValidConnections(connList)
		// Clean up connRegistry for each expired connection (AUDIT M4 fix).
		for _, pc := range expired {
			delete(p.connRegistry, pc.conn)
		}
		toClose = append(toClose, expired...)
		p.updateConnectionMap(addr, validConns)
	}
	p.mu.Unlock()

	// Close expired connections outside the lock
	for _, pc := range toClose {
		pc.closeExpired()
	}
}

// filterValidConnections separates valid connections from expired ones.
// Returns two slices: valid connections and expired connections to close.
func (p *ConnPool) filterValidConnections(connList []*PooledConn) ([]*PooledConn, []*PooledConn) {
	validConns := make([]*PooledConn, 0, len(connList))
	expiredConns := make([]*PooledConn, 0, len(connList))

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
// When the last connection for an address is removed, the address key is
// deleted from p.conns. The per-address dialMu entry is intentionally NOT
// removed here: deleting it would create a TOCTOU race where a goroutine
// that already obtained the old mutex via LoadOrStore proceeds to dial
// concurrently with another goroutine that stores a fresh mutex for the
// same address — violating the one-at-a-time serialization guarantee
// (AUDIT H3 fix).
func (p *ConnPool) updateConnectionMap(addr string, validConns []*PooledConn) {
	if len(validConns) == 0 {
		delete(p.conns, addr)
	} else {
		p.conns[addr] = validConns
	}
}
