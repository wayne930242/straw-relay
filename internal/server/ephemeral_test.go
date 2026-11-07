package server_test

import (
	"testing"

	"github.com/wayne930242/straw-relay/internal/linktest"
	"github.com/wayne930242/straw-relay/internal/server"
)

// ephTo posts a unit-status of dave to the given addressee ids at ts.
func ephTo(t *testing.T, h *harness, x *team3, to []string, ts int64, ct int) (int, map[string]any) {
	t.Helper()
	return h.send(x, x.dave, h.envelope(x, linktest.Env{From: x.dave, Kind: "unit-status", To: to, TS: ts, CT: linktest.Filler(ct)}))
}

func envelopeCount(h *harness, x *team3, dev *linktest.Device) int {
	return countEnvelopes(h.stream(dev, x.Team, "").drain())
}

// Lists that name the same members, in any order, with repeats, or with ids that are not members, are one addressee set: one row, replaced.
func TestEphemeralKeyIsTheCanonicalRecipientSet(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	ghost := linktest.NewDevice(9).ID
	lists := [][]string{
		{x.bob.ID, x.alice.ID},
		{x.alice.ID, x.bob.ID},
		{x.alice.ID, x.alice.ID, x.bob.ID},
		{x.bob.ID, x.alice.ID, ghost},
		{ghost, x.alice.ID, x.bob.ID, x.dave.ID}, // the sender is never a recipient
	}
	var one int64
	for i, to := range lists {
		if status, out := ephTo(t, h, x, to, NOW+int64(i)+1, 100); status != 201 {
			t.Fatalf("post %d: %d %v", i, status, out)
		}
		if i == 0 {
			one = h.stored(x.ID)
		}
	}
	if n := envelopeCount(h, x, x.alice); n != 1 {
		t.Errorf("alice sees %d unit-status envelopes, want 1", n)
	}
	if n := envelopeCount(h, x, x.bob); n != 1 {
		t.Errorf("bob sees %d unit-status envelopes, want 1", n)
	}
	// An older post to a reordered list is stale: it names the same set.
	if status, out := ephTo(t, h, x, []string{x.alice.ID, x.bob.ID}, NOW+2, 100); status != 200 || out["stale"] != true {
		t.Errorf("older post to the same set: %d %v", status, out)
	}
	if n := h.stored(x.ID); n >= 2*one {
		t.Errorf("stored %d bytes, want about one envelope (%d)", n, one)
	}
}

// One member that varies `to` (here with ids that are not members) must not fill the team: another member's queued envelope still fits.
func TestEphemeralFloodDoesNotFillTheTeam(t *testing.T) {
	h := newHarness(t, func(o *server.Options) { o.MaxTeamBytes = 8_000 })
	x := h.team3()
	for i := 0; i < 20; i++ {
		to := []string{x.bob.ID, linktest.NewDevice(byte(20 + i)).ID}
		if status, out := ephTo(t, h, x, to, NOW+int64(i)+1, 1000); status != 201 {
			t.Fatalf("flood post %d: %d %v", i, status, out)
		}
	}
	if status, out := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, CT: linktest.Filler(3000)})); status != 201 {
		t.Errorf("a queued envelope after the flood: %d %v", status, out)
	}
}

// A device that leaves takes the ephemerals addressed only to it; a set that named it and others keeps them for the others, under the key
// of the smaller set.
func TestRemovalDropsEphemeralsAddressedToTheRemovedDevice(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	if status, out := ephTo(t, h, x, []string{x.bob.ID}, NOW+1, 100); status != 201 {
		t.Fatalf("to bob: %d %v", status, out)
	}
	if status, out := ephTo(t, h, x, []string{x.bob.ID, x.alice.ID}, NOW+2, 100); status != 201 {
		t.Fatalf("to bob and alice: %d %v", status, out)
	}
	before := h.stored(x.ID)
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	if n := h.stored(x.ID); n >= before*3/4 {
		t.Errorf("stored %d bytes after the removal (%d before), want only the row that still names alice", n, before)
	}
	if n := envelopeCount(h, x, x.alice); n != 1 {
		t.Errorf("alice sees %d, want 1 (the set that named her too)", n)
	}
	// The rewritten row now has the key {alice}: dave's next post to alice replaces it.
	if status, out := ephTo(t, h, x, []string{x.alice.ID}, NOW+3, 100); status != 201 {
		t.Fatalf("to alice: %d %v", status, out)
	}
	if n := envelopeCount(h, x, x.alice); n != 1 {
		t.Errorf("alice sees %d after dave's new status, want 1", n)
	}
}

// A sender holds at most one row per member plus one for the whole team: with five members, the eight sets that include alice leave six.
func TestEphemeralRowsAreBoundedByTheMemberCount(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	e1, e2 := linktest.NewDevice(5), linktest.NewDevice(6)
	h.mustPost(x.alice, x.Team, x.Add(x.alice, e1, true))
	h.mustPost(x.alice, x.Team, x.Add(x.alice, e2, true))
	rest := []string{x.bob.ID, e1.ID, e2.ID}
	for mask := 0; mask < 8; mask++ {
		to := []string{x.alice.ID}
		for i, id := range rest {
			if mask&(1<<i) != 0 {
				to = append(to, id)
			}
		}
		if status, out := ephTo(t, h, x, to, NOW+int64(mask)+1, 100); status != 201 {
			t.Fatalf("set %d: %d %v", mask, status, out)
		}
	}
	if n := envelopeCount(h, x, x.alice); n != 6 {
		t.Errorf("alice sees %d of dave's statuses, want 6 (5 members + 1)", n)
	}
}
