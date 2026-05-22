package pool

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- GetOrDial tests ---

func TestGetOrDial_ReturnsExistingConnection(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	conn := newMockConn("10.0.0.1:1234")
	if err := pool.Put(conn); err != nil {
		t.Fatalf("Put: %v", err)
	}

	dialCalled := false
	got, err := pool.GetOrDial(context.Background(), "10.0.0.1:1234", func(ctx context.Context) (net.Conn, error) {
		dialCalled = true
		return newMockConn("10.0.0.1:1234"), nil
	})
	if err != nil {
		t.Fatalf("GetOrDial: %v", err)
	}
	if dialCalled {
		t.Error("dial should not have been called when a pooled connection exists")
	}
	if got == nil {
		t.Fatal("expected a connection, got nil")
	}
}

func TestGetOrDial_DialsWhenEmpty(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	dialCalled := false
	dialedConn := newMockConn("10.0.0.2:5678")
	got, err := pool.GetOrDial(context.Background(), "10.0.0.2:5678", func(ctx context.Context) (net.Conn, error) {
		dialCalled = true
		return dialedConn, nil
	})
	if err != nil {
		t.Fatalf("GetOrDial: %v", err)
	}
	if !dialCalled {
		t.Error("dial should have been called when pool is empty")
	}
	if got == nil {
		t.Fatal("expected a connection, got nil")
	}

	// The returned connection should be checked out (in use).
	stats := pool.Stats()
	if stats["total"] != 1 {
		t.Errorf("expected total=1, got %d", stats["total"])
	}
	if stats["in_use"] != 1 {
		t.Errorf("expected in_use=1, got %d", stats["in_use"])
	}
}

func TestGetOrDial_DialErrorPropagated(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	dialErr := errors.New("connection refused")
	got, err := pool.GetOrDial(context.Background(), "10.0.0.3:9999", func(ctx context.Context) (net.Conn, error) {
		return nil, dialErr
	})
	if got != nil {
		t.Error("expected nil connection on dial error")
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("expected error to contain 'connection refused', got: %v", err)
	}
}

func TestGetOrDial_ContextCancelled(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	got, err := pool.GetOrDial(ctx, "10.0.0.4:1111", func(ctx context.Context) (net.Conn, error) {
		t.Error("dial should not be called when context is cancelled")
		return newMockConn("10.0.0.4:1111"), nil
	})
	if got != nil {
		t.Error("expected nil connection on cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

func TestGetOrDial_SerializesDialsPerAddress(t *testing.T) {
	pool := newTestPool(10)
	defer pool.Close()

	const addr = "10.0.0.5:2222"
	var dialCount int32
	var maxConcurrentDials int32
	var currentDials int32

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pool.GetOrDial(context.Background(), addr, func(ctx context.Context) (net.Conn, error) {
				atomic.AddInt32(&dialCount, 1)
				cur := atomic.AddInt32(&currentDials, 1)
				// Track max concurrent dials
				for {
					old := atomic.LoadInt32(&maxConcurrentDials)
					if cur <= old || atomic.CompareAndSwapInt32(&maxConcurrentDials, old, cur) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt32(&currentDials, -1)
				return newMockConn(addr), nil
			})
			if err != nil {
				return
			}
			// Release so others can reuse
			conn.Close()
		}()
	}
	wg.Wait()

	// The key guarantee: at most 1 dial at a time for the same address.
	maxConc := atomic.LoadInt32(&maxConcurrentDials)
	if maxConc > 1 {
		t.Errorf("expected max 1 concurrent dial for same address, got %d", maxConc)
	}
}

func TestGetOrDial_DifferentAddressesDialConcurrently(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	var wg sync.WaitGroup
	var dialCount int32

	addrs := []string{"10.0.0.1:1111", "10.0.0.2:2222", "10.0.0.3:3333"}
	for _, addr := range addrs {
		addr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pool.GetOrDial(context.Background(), addr, func(ctx context.Context) (net.Conn, error) {
				atomic.AddInt32(&dialCount, 1)
				time.Sleep(10 * time.Millisecond)
				return newMockConn(addr), nil
			})
			if err != nil {
				t.Errorf("GetOrDial(%s): %v", addr, err)
			}
		}()
	}
	wg.Wait()

	if count := atomic.LoadInt32(&dialCount); count != 3 {
		t.Errorf("expected 3 dials (one per address), got %d", count)
	}
}

func TestGetOrDial_PoolClosed(t *testing.T) {
	pool := newTestPool(5)
	pool.Close()

	_, err := pool.GetOrDial(context.Background(), "10.0.0.6:3333", func(ctx context.Context) (net.Conn, error) {
		return newMockConn("10.0.0.6:3333"), nil
	})
	if err == nil {
		t.Fatal("expected error when pool is closed")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("expected 'closed' in error, got: %v", err)
	}
}

func TestGetOrDial_PoolFull(t *testing.T) {
	pool := newTestPool(1)
	defer pool.Close()

	// Fill the pool for this address
	if err := pool.Put(newMockConn("10.0.0.7:4444")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Check it out so it's in-use
	got := pool.Get("10.0.0.7:4444")
	if got == nil {
		t.Fatal("expected a connection from Get")
	}

	// Now the pool is at capacity (1 in-use for this address).
	// GetOrDial should attempt dial (since Get sees the connection as in-use),
	// but putAndGet should reject due to capacity.
	_, err := pool.GetOrDial(context.Background(), "10.0.0.7:4444", func(ctx context.Context) (net.Conn, error) {
		return newMockConn("10.0.0.7:4444"), nil
	})
	if err == nil {
		t.Fatal("expected error when pool is full")
	}
	if !strings.Contains(err.Error(), "capacity") {
		t.Errorf("expected 'capacity' in error, got: %v", err)
	}
}

func TestGetOrDial_ConnectionReleasedAndReused(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	const addr = "10.0.0.8:5555"
	var dialCount int32

	// First call: dial
	conn1, err := pool.GetOrDial(context.Background(), addr, func(ctx context.Context) (net.Conn, error) {
		atomic.AddInt32(&dialCount, 1)
		return newMockConn(addr), nil
	})
	if err != nil {
		t.Fatalf("first GetOrDial: %v", err)
	}

	// Release the connection
	if err := conn1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Second call: should reuse the released connection
	conn2, err := pool.GetOrDial(context.Background(), addr, func(ctx context.Context) (net.Conn, error) {
		atomic.AddInt32(&dialCount, 1)
		return newMockConn(addr), nil
	})
	if err != nil {
		t.Fatalf("second GetOrDial: %v", err)
	}
	if conn2 == nil {
		t.Fatal("expected a connection, got nil")
	}

	if count := atomic.LoadInt32(&dialCount); count != 1 {
		t.Errorf("expected exactly 1 dial, got %d", count)
	}
}

// --- ReadyCheck tests ---

func TestPut_ReadyCheckPasses(t *testing.T) {
	pool := NewConnPool(&PoolConfig{
		MaxSize: 5,
		MaxAge:  time.Hour,
		MaxIdle: time.Hour,
		ReadyCheck: func(c net.Conn) bool {
			return true // always ready
		},
	})
	defer pool.Close()

	conn := newMockConn("10.0.1.1:1111")
	if err := pool.Put(conn); err != nil {
		t.Fatalf("Put should succeed when ReadyCheck passes: %v", err)
	}

	stats := pool.Stats()
	if stats["total"] != 1 {
		t.Errorf("expected total=1, got %d", stats["total"])
	}
}

func TestPut_ReadyCheckFails(t *testing.T) {
	pool := NewConnPool(&PoolConfig{
		MaxSize: 5,
		MaxAge:  time.Hour,
		MaxIdle: time.Hour,
		ReadyCheck: func(c net.Conn) bool {
			return false // never ready
		},
	})
	defer pool.Close()

	conn := newMockConn("10.0.1.2:2222")
	err := pool.Put(conn)
	if err == nil {
		t.Fatal("Put should fail when ReadyCheck returns false")
	}
	if !strings.Contains(err.Error(), "ReadyCheck") {
		t.Errorf("expected error to mention ReadyCheck, got: %v", err)
	}

	stats := pool.Stats()
	if stats["total"] != 0 {
		t.Errorf("expected total=0 (connection rejected), got %d", stats["total"])
	}
}

func TestPut_NoReadyCheck(t *testing.T) {
	pool := NewConnPool(&PoolConfig{
		MaxSize: 5,
		MaxAge:  time.Hour,
		MaxIdle: time.Hour,
		// ReadyCheck is nil — all connections accepted
	})
	defer pool.Close()

	conn := newMockConn("10.0.1.3:3333")
	if err := pool.Put(conn); err != nil {
		t.Fatalf("Put should succeed without ReadyCheck: %v", err)
	}
}

func TestGetOrDial_ReadyCheckFails(t *testing.T) {
	pool := NewConnPool(&PoolConfig{
		MaxSize: 5,
		MaxAge:  time.Hour,
		MaxIdle: time.Hour,
		ReadyCheck: func(c net.Conn) bool {
			return false // never ready
		},
	})
	defer pool.Close()

	_, err := pool.GetOrDial(context.Background(), "10.0.1.4:4444", func(ctx context.Context) (net.Conn, error) {
		return newMockConn("10.0.1.4:4444"), nil
	})
	if err == nil {
		t.Fatal("GetOrDial should fail when ReadyCheck returns false")
	}
	if !strings.Contains(err.Error(), "ReadyCheck") {
		t.Errorf("expected error to mention ReadyCheck, got: %v", err)
	}
}

func TestPut_ReadyCheckWithWrappedConn(t *testing.T) {
	var checkedConn net.Conn
	pool := NewConnPool(&PoolConfig{
		MaxSize: 5,
		MaxAge:  time.Hour,
		MaxIdle: time.Hour,
		ReadyCheck: func(c net.Conn) bool {
			checkedConn = c
			return true
		},
	})
	defer pool.Close()

	inner := newMockConn("10.0.1.5:5555")
	wrapper := &PoolConnWrapper{Conn: inner, pool: pool, addr: "10.0.1.5:5555"}

	if err := pool.Put(wrapper); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// ReadyCheck should receive the unwrapped connection
	if checkedConn != inner {
		t.Error("ReadyCheck should receive the unwrapped inner connection, not the wrapper")
	}
}
