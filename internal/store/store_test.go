package store

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func open(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// A function that panics inside a transaction rolls it back, so the write lock is not held and the next write goes through at once.
func TestTxRollsBackOnPanic(t *testing.T) {
	st := open(t)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic must propagate")
			}
		}()
		_ = st.tx(func(tx *sql.Tx) error {
			if _, err := tx.Exec(`INSERT INTO teams (id, created_at) VALUES (x'01', 1)`); err != nil {
				t.Error(err)
			}
			panic("boom")
		})
	}()
	start := time.Now()
	if _, err := st.CreateToken(1); err != nil {
		t.Fatalf("a write after a panicked transaction: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("the write waited %v for a lock", d)
	}
	var n int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM teams`).Scan(&n); err != nil || n != 0 {
		t.Errorf("the panicked transaction left %d teams (%v)", n, err)
	}
}
