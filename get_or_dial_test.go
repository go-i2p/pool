package pool

import (
	"context"
	"errors"
	"fmt"
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

	// AUDIT L-5: verify total dial count equals 1 — the serialization guarantee
	// means only the first goroutine should dial; all subsequent goroutines must
	// find the pooled connection and reuse it without dialing.
	totalDials := atomic.LoadInt32(&dialCount)
	if totalDials != 1 {
		t.Errorf("expected exactly 1 total dial for same address, got %d (TOCTOU race may have occurred)", totalDials)
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

// TestDialMu_PersistsAfterCleanup validates AUDIT H3 fix:
// dialMu entries must NOT be removed during cleanup cycles — removing them
// would invalidate the one-at-a-time serialization guarantee in GetOrDial
// by creating a TOCTOU race between goroutines calling LoadOrStore.
// The accepted tradeoff is O(unique-addresses-ever-dialed) memory.
func TestDialMu_PersistsAfterCleanup(t *testing.T) {
	pool := NewConnPool(&PoolConfig{
		MaxSize: 5,
		MaxAge:  50 * time.Millisecond,
		MaxIdle: 25 * time.Millisecond,
	})
	defer pool.Close()

	// Helper to count dialMu entries
	countDialMu := func() int {
		count := 0
		pool.dialMu.Range(func(key, value interface{}) bool {
			count++
			return true
		})
		return count
	}

	// Dial 10 unique addresses successfully
	addrs := make([]string, 10)
	for i := 0; i < 10; i++ {
		addrs[i] = fmt.Sprintf("10.0.%d.1:%d", i, 8000+i)
		conn, err := pool.GetOrDial(context.Background(), addrs[i], func(ctx context.Context) (net.Conn, error) {
			return newMockConn(addrs[i]), nil
		})
		if err != nil {
			t.Fatalf("GetOrDial %d: %v", i, err)
		}
		conn.Close() // return to pool
	}

	// Verify dialMu has 10 entries
	if count := countDialMu(); count != 10 {
		t.Errorf("Expected 10 dialMu entries, got %d", count)
	}

	// Wait for connections to expire
	time.Sleep(100 * time.Millisecond)

	// Run cleanup cycle — connections expire but dialMu entries persist (H3 fix)
	pool.performCleanupCycle()

	// Verify connections are removed
	stats := pool.Stats()
	if stats["total"] != 0 {
		t.Errorf("Expected 0 connections after cleanup, got %d", stats["total"])
	}

	// dialMu entries must persist after cleanup to prevent TOCTOU races (AUDIT H3 fix)
	if count := countDialMu(); count != 10 {
		t.Errorf("Expected 10 dialMu entries to persist after cleanup (AUDIT H3 fix), got %d", count)
	}

	// Verify pool still works after cleanup
	addr := "10.0.99.1:9999"
	conn, err := pool.GetOrDial(context.Background(), addr, func(ctx context.Context) (net.Conn, error) {
		return newMockConn(addr), nil
	})
	if err != nil {
		t.Fatalf("GetOrDial after cleanup: %v", err)
	}
	if conn == nil {
		t.Fatal("Expected connection, got nil")
	}
	conn.Close()

	// 11 total: 10 old + 1 new
	if count := countDialMu(); count != 11 {
		t.Errorf("Expected 11 dialMu entries after new dial, got %d", count)
	}
}

// TestDialMu_PersistsAfterFailedDials validates that failed dials do not leave
// orphaned dialMu entries that get cleaned up — they persist intentionally to
// serialize future retry dials (AUDIT H3 fix).
func TestDialMu_PersistsAfterFailedDials(t *testing.T) {
	pool := NewConnPool(&PoolConfig{
		MaxSize: 5,
		MaxAge:  50 * time.Millisecond,
		MaxIdle: 25 * time.Millisecond,
	})
	defer pool.Close()

	// Helper to count dialMu entries
	countDialMu := func() int {
		count := 0
		pool.dialMu.Range(func(key, value interface{}) bool {
			count++
			return true
		})
		return count
	}

	// Attempt to dial 10 unique addresses that all fail
	for i := 0; i < 10; i++ {
		addr := fmt.Sprintf("10.0.%d.1:%d", i, 7000+i)
		_, err := pool.GetOrDial(context.Background(), addr, func(ctx context.Context) (net.Conn, error) {
			return nil, errors.New("dial failed")
		})
		if err == nil {
			t.Fatalf("Expected dial error for address %d", i)
		}
	}

	// Failed dials create dialMu entries (to serialize retries)
	if count := countDialMu(); count != 10 {
		t.Errorf("Expected 10 dialMu entries after failed dials, got %d", count)
	}

	// Run cleanup cycle — dialMu entries persist intentionally (AUDIT H3 fix)
	pool.performCleanupCycle()

	// dialMu entries must remain after cleanup to prevent future TOCTOU races
	if count := countDialMu(); count != 10 {
		t.Errorf("Expected 10 dialMu entries to persist after cleanup (AUDIT H3 fix), got %d", count)
	}
}

// TestGetOrDial_DialReturnsConnAndError validates AUDIT M-4 fix:
// dial callback returning both connection and error is rejected.
func TestGetOrDial_DialReturnsConnAndError(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	addr := "10.0.0.99:9999"
	conn, err := pool.GetOrDial(context.Background(), addr, func(ctx context.Context) (net.Conn, error) {
		// Invalid: return both connection and error
		return newMockConn(addr), errors.New("some error")
	})

	if conn != nil {
		t.Error("Expected nil connection when dial returns both conn and error")
	}
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !strings.Contains(err.Error(), "both connection and error") {
		t.Errorf("Expected error about returning both, got: %v", err)
	}

	// Verify no connection was added to pool
	stats := pool.Stats()
	if stats["total"] != 0 {
		t.Errorf("Expected 0 connections after invalid dial result, got %d", stats["total"])
	}
}
