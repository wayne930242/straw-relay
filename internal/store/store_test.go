package store

import (
	"database/sql"
	"math"
	"os"
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
	if got, err := st.RaiseEventSequence(500); err != nil || got != 500 { // no events yet: no sqlite_sequence row
		t.Fatalf("raise returned %d, %v, want 500", got, err)
	}
	if got := eventSeq(t, st); got != 500 {
		t.Fatalf("sequence %d, want 500", got)
	}
	if got, err := st.RaiseEventSequence(100); err != nil || got != 500 {
		t.Fatalf("a lower target returned %d, %v, want the unchanged 500", got, err)
	}
	if got := eventSeq(t, st); got != 500 {
		t.Errorf("sequence %d after a lower value, want 500", got)
	}
	if _, err := st.RaiseEventSequence(900); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO events (team, typ, body, size) VALUES (x'01', 'roster', x'00', 1)`); err != nil {
		t.Fatal(err)
	}
	if got := eventSeq(t, st); got != 901 {
		t.Errorf("next event id %d, want 901", got)
	}
}

// A second restore starts from a snapshot whose counter an earlier restore already raised; --by moves it again where a fixed target would not.
func TestRaiseEventSequenceByAlwaysMoves(t *testing.T) {
	st := open(t)
	for i, want := range []int64{1000, 2000} {
		got, err := st.RaiseEventSequenceBy(1000)
		if err != nil || got != want {
			t.Fatalf("raise %d returned %d, %v, want %d", i+1, got, err, want)
		}
		if seq := eventSeq(t, st); seq != want {
			t.Fatalf("stored counter %d, want %d", seq, want)
		}
	}
	if got, err := st.RaiseEventSequence(1000); err != nil || got != 2000 {
		t.Errorf("a fixed target below the counter returned %d, %v, want the unchanged 2000", got, err)
	}
	if _, err := st.RaiseEventSequenceBy(0); err == nil {
		t.Error("an increase of 0 must fail")
	}
	if _, err := st.RaiseEventSequenceBy(math.MaxInt64); err == nil {
		t.Error("an overflowing increase must fail")
	}
	if got := eventSeq(t, st); got != 2000 {
		t.Errorf("failed raises changed the counter to %d", got)
	}
}

func TestOpenExistingFailsOnAMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "typo.db")
	if _, err := OpenExisting(path); err == nil {
		t.Fatal("a missing file must fail")
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("OpenExisting created the file")
	}
	// The mode=rw URI flag is the second line of defence: a file removed after the stat check still fails.
	if _, err := openDB(path, "rw"); err == nil {
		t.Error("mode=rw must not create a missing file")
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("mode=rw created the file")
	}
	made := filepath.Join(t.TempDir(), "real.db")
	st, err := Open(made)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	existing, err := OpenExisting(made)
	if err != nil {
		t.Fatalf("an existing file must open: %v", err)
	}
	existing.Close()
}

// The snapshot is written beside its final name and renamed: no "*.db" file appears until it is complete, a stale ".tmp" from a killed run is replaced,
// and a failed backup leaves nothing behind.
func TestBackupWritesATempFileAndRenames(t *testing.T) {
	st := open(t)
	dir := t.TempDir()
	out := filepath.Join(dir, "snap.db")
	if err := os.WriteFile(out+".tmp", []byte("partial"), 0o600); err != nil { // what a SIGKILLed VACUUM INTO leaves
		t.Fatal(err)
	}
	if err := st.Backup(out); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != "snap.db" {
		t.Errorf("directory holds %v, want only snap.db", entries)
	}
	snap, err := Open(out)
	if err != nil {
		t.Fatalf("the snapshot does not open: %v", err)
	}
	snap.Close()

	missing := filepath.Join(dir, "no-such-dir", "snap.db")
	if err := st.Backup(missing); err == nil {
		t.Error("a missing output directory must fail")
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "*", "*")); len(m) != 0 {
		t.Errorf("a failed backup left %v", m)
	}
}
