package pool

import (
	"context"
	"sync"
	"testing"
	"time"
)

// --- Drain tests ---

func TestDrain_ReturnsImmediatelyWhenNoInUse(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")

	// Connection is idle (not checked out).
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := f.pool.Drain(ctx); err != nil {
		t.Fatalf("Drain should return nil when no connections are in use: %v", err)
	}
}

func TestDrain_ReturnsImmediatelyWhenPoolEmpty(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain should return nil for empty pool: %v", err)
	}
}

func TestDrain_WaitsForInUseConnections(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")
	f.checkout(t)

	stats := f.pool.Stats()
	if stats["in_use"] != 1 {
		t.Fatalf("expected 1 in_use, got %d", stats["in_use"])
	}

	// Release after a short delay in a goroutine.
	go func() {
		time.Sleep(100 * time.Millisecond)
		f.pool.Release(f.addr, f.conn)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	if err := f.pool.Drain(ctx); err != nil {
		t.Fatalf("Drain should succeed after release: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 50*time.Millisecond {
		t.Errorf("Drain returned too quickly (%v), expected to wait for release", elapsed)
	}
}

func TestDrain_ContextCancelled(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")
	f.checkout(t) // Check out and never release.

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err := f.pool.Drain(ctx)
	if err == nil {
		t.Fatal("Drain should return an error when context is cancelled")
	}

	if ctx.Err() == nil {
		t.Fatal("context should be cancelled")
	}
}

func TestDrain_MultipleInUseConnections(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	addrs := []string{"10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80"}
	conns := make([]*mockConn, len(addrs))
	for i, addr := range addrs {
		conns[i] = newMockConn(addr)
		if err := pool.Put(conns[i]); err != nil {
			t.Fatalf("Put(%s): %v", addr, err)
		}
		got := pool.Get(addr)
		if got == nil {
			t.Fatalf("Get(%s) returned nil", addr)
		}
	}

	stats := pool.Stats()
	if stats["in_use"] != 3 {
		t.Fatalf("expected 3 in_use, got %d", stats["in_use"])
	}

	// Release connections one at a time with staggered delays.
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Add(1)
		go func(idx int, a string) {
			defer wg.Done()
			time.Sleep(time.Duration(50*(idx+1)) * time.Millisecond)
			pool.Release(a, conns[idx])
		}(i, addr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain should succeed after all releases: %v", err)
	}
	wg.Wait()

	stats = pool.Stats()
	if stats["in_use"] != 0 {
		t.Fatalf("expected 0 in_use after Drain, got %d", stats["in_use"])
	}
}

// TestClose_PreservesInUseForDrain verifies that Close() does not clear
// in-use connections from the map, so that Drain() can still observe and
// wait for them.
func TestClose_PreservesInUseForDrain(t *testing.T) {
	f := setupClosedPoolWithInUse(t, "10.0.0.20:80")

	// Stats should still show 1 in-use.
	stats := f.pool.Stats()
	if stats["in_use"] != 1 {
		t.Errorf("expected 1 in_use after Close(), got %d", stats["in_use"])
	}

	// Drain should wait for the in-use connection.
	go func() {
		time.Sleep(50 * time.Millisecond)
		f.wrapper.Close() // returns conn to the closed pool, which closes it
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := f.pool.Drain(ctx); err != nil {
		t.Fatalf("Drain should succeed after in-use connection is returned: %v", err)
	}

	stats = f.pool.Stats()
	if stats["in_use"] != 0 {
		t.Errorf("expected 0 in_use after Drain, got %d", stats["in_use"])
	}
}

// --- Snapshot tests ---

func TestSnapshot_EmptyPool(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	snap := pool.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected empty snapshot, got %d entries", len(snap))
	}
}

func TestSnapshot_ReturnsAllConnections(t *testing.T) {
	pool := newTestPool(5)
	defer pool.Close()

	addrs := []string{"10.0.0.1:80", "10.0.0.2:80"}
	for _, addr := range addrs {
		if err := pool.Put(newMockConn(addr)); err != nil {
			t.Fatalf("Put(%s): %v", addr, err)
		}
	}

	snap := pool.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 connections in snapshot, got %d", len(snap))
	}

	// Verify all addresses are present.
	found := make(map[string]bool)
	for _, pc := range snap {
		found[pc.Address()] = true
	}
	for _, addr := range addrs {
		if !found[addr] {
			t.Errorf("snapshot missing address %s", addr)
		}
	}
}

func TestSnapshot_IncludesInUseConnections(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")
	f.checkout(t)

	snap := f.pool.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 connection in snapshot, got %d", len(snap))
	}
	if !snap[0].IsInUse() {
		t.Error("snapshot should show connection as in-use")
	}
}

func TestSnapshot_IsShallowCopy(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")

	snap1 := f.pool.Snapshot()
	snap2 := f.pool.Snapshot()

	// Two snapshots should return separate PooledConn structs.
	if snap1[0] == snap2[0] {
		t.Error("snapshots should return distinct PooledConn pointers")
	}

	// But they should reference the same underlying net.Conn.
	if snap1[0].NetConn() != snap2[0].NetConn() {
		t.Error("snapshots should reference the same underlying net.Conn")
	}
}

// --- PooledConn accessor tests ---

func TestPooledConn_NetConn(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")

	snap := f.pool.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap))
	}
	if snap[0].NetConn() != f.conn {
		t.Error("NetConn() should return the original connection")
	}
}

func TestPooledConn_CreatedAt(t *testing.T) {
	before := time.Now()
	f := setupPoolWithConn(t, "10.0.0.1:80")
	after := time.Now()

	snap := f.pool.Snapshot()
	created := snap[0].CreatedAt()
	if created.Before(before) || created.After(after) {
		t.Errorf("CreatedAt() %v should be between %v and %v", created, before, after)
	}
}

func TestPooledConn_LastUsedAt(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")

	snap := f.pool.Snapshot()
	lastUsed := snap[0].LastUsedAt()
	if lastUsed.IsZero() {
		t.Error("LastUsedAt() should not be zero")
	}

	// Check out and release — lastUsed should advance.
	time.Sleep(10 * time.Millisecond)
	got := f.checkout(t)
	_ = got

	snap2 := f.pool.Snapshot()
	if !snap2[0].LastUsedAt().After(lastUsed) {
		t.Error("LastUsedAt() should advance after Get()")
	}
}

func TestPooledConn_IsInUse(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")

	snap := f.pool.Snapshot()
	if snap[0].IsInUse() {
		t.Error("connection should not be in use after Put")
	}

	f.checkout(t)

	snap = f.pool.Snapshot()
	if !snap[0].IsInUse() {
		t.Error("connection should be in use after Get")
	}

	f.pool.Release(f.addr, f.conn)

	snap = f.pool.Snapshot()
	if snap[0].IsInUse() {
		t.Error("connection should not be in use after Release")
	}
}

func TestPooledConn_Address(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")

	snap := f.pool.Snapshot()
	if snap[0].Address() != f.addr {
		t.Errorf("Address() = %q, want %q", snap[0].Address(), f.addr)
	}
}

// --- Drain + Snapshot integration ---

func TestDrain_ThenSnapshot_AllIdle(t *testing.T) {
	f := setupPoolWithConn(t, "10.0.0.1:80")
	f.checkout(t)

	go func() {
		time.Sleep(50 * time.Millisecond)
		f.pool.Release(f.addr, f.conn)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := f.pool.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	snap := f.pool.Snapshot()
	for _, pc := range snap {
		if pc.IsInUse() {
			t.Error("after Drain, no connection should be in use")
		}
	}
}
