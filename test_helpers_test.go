package pool

import (
	"net"
	"testing"
	"time"
)

// newTestPool creates a ConnPool with standard test defaults
// (MaxAge=1h, MaxIdle=1h) and the given maximum size.
func newTestPool(maxSize int) *ConnPool {
	return NewConnPool(&PoolConfig{
		MaxSize: maxSize,
		MaxAge:  time.Hour,
		MaxIdle: time.Hour,
	})
}

// poolConnFixture holds a pool with a single mock connection already Put.
type poolConnFixture struct {
	pool *ConnPool
	conn *mockConn
	addr string
}

// setupPoolWithConn creates a size-5 pool, puts a mock connection with addr,
// and registers pool.Close via t.Cleanup. The returned fixture provides
// access to the pool, mock conn, and address.
func setupPoolWithConn(t *testing.T, addr string) poolConnFixture {
	t.Helper()
	p := newTestPool(5)
	t.Cleanup(func() { p.Close() })
	c := newMockConn(addr)
	if err := p.Put(c); err != nil {
		t.Fatalf("Put(%s): %v", addr, err)
	}
	return poolConnFixture{pool: p, conn: c, addr: addr}
}

// checkout calls Get on the fixture's address and fatals if nil.
func (f poolConnFixture) checkout(t *testing.T) net.Conn {
	t.Helper()
	w := f.pool.Get(f.addr)
	if w == nil {
		t.Fatalf("Get(%q) returned nil", f.addr)
	}
	return w
}

// closedPoolWithInUse holds a pool that has been Put→Get→Close with one
// connection still in use.
type closedPoolWithInUse struct {
	pool    *ConnPool
	conn    *mockConn
	wrapper net.Conn // the checked-out wrapper
}

// setupNilPoolWithWrapped creates a default (nil-config) pool, puts a mock
// connection, and checks it out. Registers pool.Close via t.Cleanup.
func setupNilPoolWithWrapped(t *testing.T, addr string) (*ConnPool, *mockConn, net.Conn) {
	t.Helper()
	pool := NewConnPool(nil)
	t.Cleanup(func() { pool.Close() })
	conn := newMockConn(addr)
	pool.Put(conn)
	wrapped := pool.Get(addr)
	if wrapped == nil {
		t.Fatal("Get returned nil")
	}
	return pool, conn, wrapped
}

// setupClosedPoolWithInUse creates a pool, puts a mock connection, checks it
// out, then closes the pool — leaving one connection in-use. Callers can
// assert on the state of the pool and wrapper.
func setupClosedPoolWithInUse(t *testing.T, addr string) closedPoolWithInUse {
	t.Helper()
	p := newTestPool(5)
	c := newMockConn(addr)
	if err := p.Put(c); err != nil {
		t.Fatalf("Put: %v", err)
	}
	wrapper := p.Get(addr)
	if wrapper == nil {
		t.Fatal("Get returned nil")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return closedPoolWithInUse{pool: p, conn: c, wrapper: wrapper}
}
