package main

import (
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
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
