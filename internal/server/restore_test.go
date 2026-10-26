package server_test

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/wayne930242/straw-relay/internal/linktest"
)

// The restore drill of the runbook: a snapshot is restored after clients have seen higher event ids than it knows. Without raising the event counter
// before the relay serves again, a client resuming with its saved Last-Event-ID never gets the roster entries made after the restore; with it they arrive.
// (Queued envelopes are replayed whatever the id, so a roster entry is what shows the problem.)
func TestRestoredSnapshotNeedsTheEventSequenceRaised(t *testing.T) {
	live := newHarness(t)
	x := live.team3()
	dir := t.TempDir()
	plain, raised := filepath.Join(dir, "plain.db"), filepath.Join(dir, "raised.db")
	for _, p := range []string{plain, raised} { // taken while the relay serves
		if err := live.st.Backup(p); err != nil {
			t.Fatal(err)
		}
	}

	// After the snapshot bob sees more events and saves the last id; then the host is lost.
	for seq := int64(1); seq <= 5; seq++ {
		live.send(x, x.alice, live.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: seq}))
	}
	var saved string
	for _, e := range live.stream(x.bob, x.Team, "").drain() {
		saved = e.ID
	}
	savedID, err := strconv.ParseInt(saved, 10, 64)
	if err != nil {
		t.Fatalf("saved id %q: %v", saved, err)
	}

	carol := x.Add(x.alice, linktest.NewDevice(3), true) // roster entry 3, appended after either restore

	// Restored as it is: the next roster entry gets an id below the one bob holds, so his resume skips it.
	h := newHarnessAt(t, plain)
	h.mustPost(x.alice, x.Team, carol)
	if got := h.stream(x.bob, x.Team, saved).drain(); len(got) != 0 {
		t.Fatalf("without the raise bob already sees %d events; the drill no longer shows the problem", len(got))
	}

	// Restored and raised above bob's id before serving: the same roster entry reaches him.
	r := newHarnessAt(t, raised)
	if _, err := r.st.RaiseEventSequence(savedID + 1000); err != nil {
		t.Fatal(err)
	}
	r.mustPost(x.alice, x.Team, carol)
	got := r.stream(x.bob, x.Team, saved).drain()
	if len(got) != 1 || got[0].Name != "roster" || got[0].Data["seq"] != float64(3) {
		t.Fatalf("after the raise bob sees %v, want the one new roster entry", got)
	}
	if id, _ := strconv.ParseInt(got[0].ID, 10, 64); id <= savedID {
		t.Errorf("new event id %d is not above the saved %d", id, savedID)
	}
}
