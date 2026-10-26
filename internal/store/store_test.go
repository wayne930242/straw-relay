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

// A backup taken while the store is open (WAL mode, uncheckpointed writes) opens on its own and holds every row.
func TestBackupIsConsistentAndSelfContained(t *testing.T) {
	st := open(t)
	for i := 0; i < 3; i++ {
		if _, err := st.CreateToken(int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(t.TempDir(), "snap.db")
	if err := st.Backup(out); err != nil {
		t.Fatal(err)
	}
	snap, err := Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	var n int
	if err := snap.db.QueryRow(`SELECT COUNT(*) FROM tokens`).Scan(&n); err != nil || n != 3 {
		t.Errorf("snapshot has %d tokens (%v), want 3", n, err)
	}
	if err := st.Backup(out); err == nil {
		t.Error("backing up over an existing file must fail")
	}
}

func eventSeq(t *testing.T, st *Store) int64 {
	t.Helper()
	var seq int64
	if err := st.db.QueryRow(`SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)`).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	return seq
}

func TestRaiseEventSequenceOnlyRaises(t *testing.T) {
	st := open(t)
	if err := st.RaiseEventSequence(500); err != nil { // no events yet: no sqlite_sequence row
		t.Fatal(err)
	}
	if got := eventSeq(t, st); got != 500 {
		t.Fatalf("sequence %d, want 500", got)
	}
	if err := st.RaiseEventSequence(100); err != nil {
		t.Fatal(err)
	}
	if got := eventSeq(t, st); got != 500 {
		t.Errorf("sequence %d after a lower value, want 500", got)
	}
	if err := st.RaiseEventSequence(900); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO events (team, typ, body, size) VALUES (x'01', 'roster', x'00', 1)`); err != nil {
		t.Fatal(err)
	}
	if got := eventSeq(t, st); got != 901 {
		t.Errorf("next event id %d, want 901", got)
	}
}
