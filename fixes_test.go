package pool

import (
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
)

// --- Double-release vulnerability tests ---

// TestRelease_MarksWrapperClosed verifies that calling Release() with a
// PoolConnWrapper marks the wrapper's closed flag, preventing a subsequent
// Close() from issuing a second release.
func TestRelease_MarksWrapperClosed(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:5000")
	wrapper := f.checkout(t)

	// Release via the pool (passing the wrapper directly)
	if err := f.pool.Release(f.addr, wrapper); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// A subsequent Close on the wrapper must return ALREADY_CLOSED
	err := wrapper.Close()
	if err == nil {
		t.Fatal("expected error on double-close after Release, got nil")
	}
	if !strings.Contains(err.Error(), "ALREADY_CLOSED") &&
		!strings.Contains(err.Error(), "already closed") {
		t.Errorf("expected ALREADY_CLOSED error, got: %v", err)
	}
}

// TestRelease_ThenClose_NoDoubleRelease verifies the full double-release
// scenario: Get -> Release(wrapper) -> wrapper.Close() must not allow a
// second goroutine to observe the connection as idle between the two calls.
func TestRelease_ThenClose_NoDoubleRelease(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.2:6000")
	wrapper := f.checkout(t)

	// First release via pool
	if err := f.pool.Release(f.addr, wrapper); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// Another goroutine gets the now-idle connection
	wrapper2 := f.pool.Get(f.addr)
	if wrapper2 == nil {
		t.Fatal("second Get returned nil - connection should be idle after Release")
	}

	// The original wrapper.Close() must fail (already closed by Release)
	err := wrapper.Close()
	if err == nil {
		t.Fatal("wrapper.Close() should fail after Release already marked it closed")
	}

	// The second wrapper should still be usable
	if err := wrapper2.Close(); err != nil {
		t.Errorf("second wrapper Close failed: %v", err)
	}
}

// TestRelease_ConcurrentWithClose_NoRace exercises the double-release fix
// under concurrent access to catch data races.
func TestRelease_ConcurrentWithClose_NoRace(t *testing.T) {
	p := newTestPool(10)
	defer p.Close()

	const iterations = 100
	for i := 0; i < iterations; i++ {
		conn := newMockConn("10.0.0.3:7000")
		if err := p.Put(conn); err != nil {
			t.Fatalf("Put: %v", err)
		}

		wrapper := p.Get("10.0.0.3:7000")
		if wrapper == nil {
			continue // pool may be at capacity
		}

		var wg sync.WaitGroup
		wg.Add(2)

		// Race Release and Close against each other
		go func() {
			defer wg.Done()
			p.Release("10.0.0.3:7000", wrapper) //nolint:errcheck
		}()
		go func() {
			defer wg.Done()
			wrapper.Close() //nolint:errcheck
		}()

		wg.Wait()
	}
}

// TestRelease_WithRawConn_NoWrapperMarking verifies that Release with a
// non-wrapper conn works normally (no wrapper marking logic triggered).
func TestRelease_WithRawConn_NoWrapperMarking(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.4:8000")
	wrapper := f.checkout(t)
	pcw := wrapper.(*PoolConnWrapper)
	rawConn := pcw.Conn

	// Release with the raw conn should work
	if err := f.pool.Release(f.addr, rawConn); err != nil {
		t.Fatalf("Release with raw conn: %v", err)
	}

	// Connection should be available again
	wrapper2 := f.pool.Get(f.addr)
	if wrapper2 == nil {
		t.Error("connection should be available after Release")
	}
}

// --- RemoteAddr nil-panic guard tests ---

// nilAddrConn returns nil from RemoteAddr()
type nilAddrConn struct {
	mockConn
}

func (n *nilAddrConn) RemoteAddr() net.Addr { return nil }

// TestPut_NilRemoteAddr verifies that Put returns an error instead of
// panicking when the connection's RemoteAddr() returns nil.
func TestPut_NilRemoteAddr(t *testing.T) {
	p := newTestPool(5)
	defer p.Close()

	conn := &nilAddrConn{}
	err := p.Put(conn)
	if err == nil {
		t.Fatal("expected error for nil RemoteAddr, got nil")
	}
	if !strings.Contains(err.Error(), "nil RemoteAddr") {
		t.Errorf("expected 'nil RemoteAddr' in error, got: %v", err)
	}
}

// TestPut_NilRemoteAddr_WrappedConn verifies the nil guard also works when
// the connection is wrapped in a PoolConnWrapper.
func TestPut_NilRemoteAddr_WrappedConn(t *testing.T) {
	p := newTestPool(5)
	defer p.Close()

	inner := &nilAddrConn{}
	wrapper := &PoolConnWrapper{Conn: inner, pool: p, addr: "fake"}
	err := p.Put(wrapper)
	if err == nil {
		t.Fatal("expected error for nil RemoteAddr on unwrapped conn, got nil")
	}
	if !strings.Contains(err.Error(), "nil RemoteAddr") {
		t.Errorf("expected 'nil RemoteAddr' in error, got: %v", err)
	}
}

// --- Close() error collection tests ---

// failingCloseConn returns an error from Close()
type failingCloseConn struct {
	mockConn
	closeErr error
}

func (f *failingCloseConn) Close() error {
	f.closed = true
	return f.closeErr
}

func newFailingCloseConn(addr string, err error) *failingCloseConn {
	return &failingCloseConn{
		mockConn: mockConn{
			localAddr:  &mockAddr{network: "tcp", address: "127.0.0.1:0"},
			remoteAddr: &mockAddr{network: "tcp", address: addr},
		},
		closeErr: err,
	}
}

// TestClose_CollectsErrors verifies that ConnPool.Close() returns an
// aggregated error containing all individual Close() failures.
func TestClose_CollectsErrors(t *testing.T) {
	p := newTestPool(10)

	err1 := errors.New("close error 1")
	err2 := errors.New("close error 2")
	err3 := errors.New("close error 3")

	if err := p.Put(newFailingCloseConn("10.0.0.1:1001", err1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := p.Put(newFailingCloseConn("10.0.0.1:1002", err2)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := p.Put(newFailingCloseConn("10.0.0.2:1003", err3)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	closeErr := p.Close()
	if closeErr == nil {
		t.Fatal("expected aggregated error from Close(), got nil")
	}

	// All three errors should be present in the aggregated error
	if !errors.Is(closeErr, err1) {
		t.Errorf("aggregated error missing err1: %v", closeErr)
	}
	if !errors.Is(closeErr, err2) {
		t.Errorf("aggregated error missing err2: %v", closeErr)
	}
	if !errors.Is(closeErr, err3) {
		t.Errorf("aggregated error missing err3: %v", closeErr)
	}
}

// TestClose_NoErrorsWhenAllSucceed verifies that Close() returns nil when
// all connection closures succeed.
func TestClose_NoErrorsWhenAllSucceed(t *testing.T) {
	p := newTestPool(5)

	if err := p.Put(newMockConn("10.0.0.5:9000")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := p.Put(newMockConn("10.0.0.5:9001")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Errorf("Close() should return nil when all close fine, got: %v", err)
	}
}

// TestClose_InUseConnectionsNotClosed verifies that in-use connections are
// not closed by Close() (they will be closed when returned via Release/Discard).
func TestClose_InUseConnectionsNotClosed(t *testing.T) {
	f := setupClosedPoolWithInUse(t, "10.0.0.6:9100")

	// The in-use connection should NOT have been closed
	if f.conn.closed {
		t.Error("in-use connection should not be closed by pool.Close()")
	}
}

// TestPoolConnWrapper_DoubleDiscard verifies that calling Discard() twice on
// the same wrapper returns an ALREADY_CLOSED error on the second call.
func TestPoolConnWrapper_DoubleDiscard(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.10:6000")
	wrapped := f.checkout(t)

	wrapper, ok := wrapped.(*PoolConnWrapper)
	if !ok {
		t.Fatal("expected PoolConnWrapper")
	}

	// First Discard should succeed.
	if err := wrapper.Discard(); err != nil {
		t.Fatalf("first Discard should succeed: %v", err)
	}

	if !f.conn.closed {
		t.Error("connection should be closed after Discard")
	}

	// Second Discard should return ALREADY_CLOSED.
	err := wrapper.Discard()
	if err == nil {
		t.Fatal("second Discard should return an error")
	}
	if !strings.Contains(err.Error(), "ALREADY_CLOSED") &&
		!strings.Contains(err.Error(), "already closed") {
		t.Errorf("expected ALREADY_CLOSED error, got: %v", err)
	}
}

// TestClose_MixedErrors verifies Close() handles a mix of successful and
// failing connection closures correctly.
func TestClose_MixedErrors(t *testing.T) {
	p := newTestPool(10)

	expectedErr := errors.New("mixed close error")
	if err := p.Put(newMockConn("10.0.0.7:9200")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := p.Put(newFailingCloseConn("10.0.0.7:9201", expectedErr)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	closeErr := p.Close()
	if closeErr == nil {
		t.Fatal("expected error from Close(), got nil")
	}
	if !errors.Is(closeErr, expectedErr) {
		t.Errorf("expected error to contain %v, got: %v", expectedErr, closeErr)
	}
}
