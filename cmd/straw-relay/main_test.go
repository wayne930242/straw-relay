package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wayne930242/straw-relay/internal/store"
)

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestTokenCreatePrintsAToken(t *testing.T) {
	var out strings.Builder
	if err := run([]string{"token", "create", "--db", filepath.Join(t.TempDir(), "t.db")}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "srt1_") {
		t.Errorf("output %q", out.String())
	}
}

// A token that could not be printed is a failure: the operator would otherwise believe they have one.
func TestTokenCreateFailsWhenPrintingFails(t *testing.T) {
	err := run([]string{"token", "create", "--db", filepath.Join(t.TempDir(), "t.db")}, failingWriter{}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Errorf("got %v, want the write error", err)
	}
}

// create makes an empty relay database, which is what the relay itself does on its first start.
func create(t *testing.T, path string) {
	t.Helper()
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupAndRaiseSequenceCommands(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "t.db")
	create(t, db)
	out := filepath.Join(dir, "snap.db")
	var stdout strings.Builder
	if err := run([]string{"backup", "--db", db, out}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), out) {
		t.Errorf("output %q", stdout.String())
	}
	if err := run([]string{"backup", "--db", db, out}, io.Discard, io.Discard); err == nil {
		t.Error("an existing output file must fail")
	}
	if err := run([]string{"backup", "--db", db}, io.Discard, io.Discard); err == nil {
		t.Error("a missing output path must fail")
	}
	stdout.Reset()
	if err := run([]string{"raise-sequence", "--db", out, "1000"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "now 1000") || !strings.Contains(stdout.String(), "1001") {
		t.Errorf("output %q", stdout.String())
	}
	if err := run([]string{"raise-sequence", "--db", out, "x"}, io.Discard, io.Discard); err == nil {
		t.Error("a non-number must fail")
	}
}

func TestBackupIntoADirectoryNamesTheFileByTime(t *testing.T) {
	dir := t.TempDir()
	create(t, filepath.Join(dir, "t.db"))
	var stdout strings.Builder
	if err := run([]string{"backup", "--db", filepath.Join(dir, "t.db"), dir}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	m, _ := filepath.Glob(filepath.Join(dir, "straw-relay-*.db"))
	if len(m) != 1 || !strings.Contains(stdout.String(), m[0]) {
		t.Errorf("files %v, output %q", m, stdout.String())
	}
}

// A second restore's snapshot already carries the counter an earlier restore raised: --by raises it again and prints the value it reached.
func TestRaiseSequenceByAddsEachTime(t *testing.T) {
	db := filepath.Join(t.TempDir(), "t.db")
	create(t, db)
	for _, want := range []string{"now 1000000000;", "now 2000000000;"} {
		var stdout strings.Builder
		if err := run([]string{"raise-sequence", "--db", db, "--by", "1000000000"}, &stdout, io.Discard); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("output %q, want it to contain %q", stdout.String(), want)
		}
	}
	for _, bad := range [][]string{
		{"raise-sequence", "--db", db, "--by", "5", "10"}, // both forms
		{"raise-sequence", "--db", db},                    // neither
		{"raise-sequence", "--db", db, "--by", "0"},       // not an increase
		{"raise-sequence", "--db", db, "--by", "-3"},
	} {
		if err := run(bad, io.Discard, io.Discard); err == nil {
			t.Errorf("%v must fail", bad)
		}
	}
}

// A mistyped --db must fail and create nothing: an empty-schema snapshot would pass for a real backup.
func TestMaintenanceCommandsFailOnAMissingDatabase(t *testing.T) {
	dir := t.TempDir()
	typo := filepath.Join(dir, "typo.db")
	outDir := t.TempDir()
	if err := run([]string{"backup", "--db", typo, outDir}, io.Discard, io.Discard); err == nil {
		t.Error("backup of a missing database must fail")
	}
	if err := run([]string{"raise-sequence", "--db", typo, "--by", "5"}, io.Discard, io.Discard); err == nil {
		t.Error("raise-sequence --by on a missing database must fail")
	}
	if err := run([]string{"raise-sequence", "--db", typo, "5"}, io.Discard, io.Discard); err == nil {
		t.Error("raise-sequence on a missing database must fail")
	}
	for _, d := range []string{dir, outDir} {
		if m, _ := os.ReadDir(d); len(m) != 0 {
			t.Errorf("%s now holds %v, want nothing", d, m)
		}
	}
}
