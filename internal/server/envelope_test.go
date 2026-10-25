package server_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wayne930242/straw-relay/internal/linktest"
	"github.com/wayne930242/straw-relay/internal/server"
)

const day = 24 * time.Hour

func TestEnvelopeReachesOnlyItsAddressees(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	bob := h.stream(x.bob, x.Team, "")
	dave := h.stream(x.dave, x.Team, "")
	alice := h.stream(x.alice, x.Team, "")
	for _, s := range []*stream{bob, dave, alice} {
		if s.status != 200 {
			t.Fatalf("stream status %d", s.status)
		}
		s.drain() // the roster entries
	}

	id := linktest.Filler(16)
	env := h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, ID: id})
	status, out := h.send(x, x.alice, env)
	if status != 201 || out["id"] != hx(id) || out["duplicate"] != false {
		t.Fatalf("send: %d %v", status, out)
	}
	e := bob.expect("envelope")
	if e.Data["id"] != hx(id) || e.Data["from"] != x.alice.ID || e.Data["ephemeral"] != false || e.Data["envelope"] != b64(env) {
		t.Errorf("event data: %v", e.Data)
	}
	if e.ID == "" {
		t.Error("an event carries an id")
	}
	dave.quiet()
	alice.quiet()

	// A retry of the same envelope id is accepted once.
	if status, out := h.send(x, x.alice, env); status != 200 || out["duplicate"] != true {
		t.Errorf("duplicate: %d %v", status, out)
	}
	bob.quiet()

	// Several addressees; a name that is not a member is ignored; only non-members is an error.
	env2 := h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID, x.dave.ID, linktest.NewDevice(9).ID}, Seq: 2})
	if status, _ := h.send(x, x.alice, env2); status != 201 {
		t.Fatalf("several: %d", status)
	}
	bob.expect("envelope")
	dave.expect("envelope")
	env3 := h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{linktest.NewDevice(9).ID}, Seq: 3})
	if status, out := h.send(x, x.alice, env3); status != 422 || out["error"] != "no-addressee" {
		t.Errorf("no member addressed: %d %v", status, out)
	}
}

func TestEnvelopeHeaderChecks(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	other := linktest.NewTeam()
	cases := []struct {
		name   string
		sender *linktest.Device
		env    linktest.Env
		status int
		reason string
	}{
		{"wrong version", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, Version: 2}, 422, "bad-version"},
		{"another team in the header", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, Team: other.ID}, 422, "wrong-team"},
		{"unknown kind", x.alice, linktest.Env{From: x.alice, Kind: "shout", To: []string{x.bob.ID}, Seq: 1}, 422, "unknown-kind"},
		{"a presence marked queued", x.alice, linktest.Env{From: x.alice, Kind: "presence", Seq: 1, Ephemeral: ptr(false)}, 422, "ephemeral-mismatch"},
		{"a tower marked ephemeral", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Ephemeral: ptr(true)}, 422, "ephemeral-mismatch"},
		{"an attended device sends an unattended kind", x.alice, linktest.Env{From: x.alice, Kind: "dialog-open", To: []string{x.bob.ID}, Seq: 1}, 422, "kind-not-allowed"},
		{"the whole team for a kind that is not for it", x.alice, linktest.Env{From: x.alice, Kind: "tower", Seq: 1}, 422, "whole-team-not-allowed"},
		{"a queued kind with seq 0", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}}, 422, "bad-seq"},
		{"an ephemeral kind with a seq", x.alice, linktest.Env{From: x.alice, Kind: "presence", Seq: 3}, 422, "bad-seq"},
		{"a recipient that is not an id", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{"nope"}, Seq: 1}, 422, "bad-header"},
		{"a stranger's envelope", x.alice, linktest.Env{From: linktest.NewDevice(9), Kind: "tower", To: []string{x.bob.ID}, Seq: 1}, 403, "from-mismatch"},
		{"signed by someone else", x.alice, linktest.Env{From: x.alice, Signer: x.bob, Kind: "tower", To: []string{x.bob.ID}, Seq: 1}, 422, "bad-signature"},
		{"another epoch", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, Epoch: 4}, 422, "bad-epoch"},
		{"a roster head ahead", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, HeadSeq: 9, HeadHash: linktest.Filler(32)}, 422, "roster-ahead"},
		{"a roster head the relay does not have", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, HeadSeq: 1, HeadHash: linktest.Filler(32)}, 422, "roster-mismatch"},
		{"a ts too far ahead", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, TS: NOW + 11*60_000}, 422, "future-ts"},
		{"a ts older than the relay keeps", x.alice, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, TS: NOW - 8*24*3600_000}, 422, "old-ts"},
	}
	for _, c := range cases {
		env := c.env
		if env.Team == nil {
			env.Team = x.ID
		}
		raw := h.envelope(x, env)
		if c.env.Team != nil { // keep the team the case chose
			raw = linktest.Envelope(withDefaults(h, x, env))
		}
		status, out := h.send(x, c.sender, raw)
		if status != c.status || out["error"] != c.reason {
			t.Errorf("%s: %d %v, want %d %s", c.name, status, out, c.status, c.reason)
		}
	}
	// Not an envelope.
	if status, out := h.send(x, x.alice, []byte("junk")); status != 400 || out["error"] != "malformed" {
		t.Errorf("junk: %d %v", status, out)
	}
	// An envelope from a member removed since is refused at the door.
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	removed := h.envelope(x, linktest.Env{From: x.bob, Kind: "tower", To: []string{x.alice.ID}, Seq: 1, Epoch: 1, HeadSeq: 2, HeadHash: x.Hash})
	if status, _ := h.send(x, x.bob, removed); status != 401 {
		t.Errorf("a removed sender: %d", status)
	}
	// The previous epoch is taken from a sender that wrote before the rotation, as the spec allows.
	before := h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.dave.ID}, Seq: 1, Epoch: 1, HeadSeq: 2, HeadHash: sha(nil)})
	if status, out := h.send(x, x.alice, before); status != 422 || out["error"] != "roster-mismatch" {
		t.Errorf("previous epoch with a wrong hash: %d %v", status, out)
	}
}

func ptr[T any](v T) *T { return &v }

func withDefaults(h *harness, x *team3, e linktest.Env) linktest.Env {
	if e.TS == 0 {
		e.TS = h.now.Load()
	}
	if e.Epoch == 0 {
		e.Epoch = x.Epoch
	}
	if e.HeadHash == nil {
		e.HeadSeq, e.HeadHash = x.Seq, x.Hash
	}
	return e
}

func TestPerSenderOrderAndAcks(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	var ids [][]byte
	for i := 1; i <= 4; i++ {
		id := linktest.Filler(16)
		ids = append(ids, id)
		if status, _ := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: int64(i), ID: id})); status != 201 {
			t.Fatal(status)
		}
	}
	s := h.stream(x.bob, x.Team, "")
	var got []string
	var lastEventID string
	for _, e := range s.drain() {
		if e.Name == "envelope" {
			got = append(got, e.Data["id"].(string))
			lastEventID = e.ID
		}
	}
	if len(got) != 4 {
		t.Fatalf("got %d envelopes", len(got))
	}
	for i, id := range ids {
		if got[i] != hx(id) {
			t.Errorf("envelope %d out of order", i)
		}
	}
	// Without an ack they come again, whether or not the client resumes past them: an envelope the device has not acknowledged is not lost to a
	// Last-Event-ID (it may have crashed before delivering it).
	s2 := h.stream(x.bob, x.Team, "")
	if n := countEnvelopes(s2.drain()); n != 4 {
		t.Errorf("unacked after reconnect: %d", n)
	}
	s3 := h.stream(x.bob, x.Team, lastEventID)
	if n := countEnvelopes(s3.drain()); n != 4 {
		t.Errorf("unacked after resuming past them: %d", n)
	}
	// Ack two: only the other two remain. An ack of an unknown id or of another device's envelope changes nothing.
	status, out := h.call(x.bob, x.ID, "POST", "/v1/teams/"+x.Hex()+"/acks", map[string]any{"ids": []string{hx(ids[0]), hx(ids[1]), hx(linktest.Filler(16))}})
	if status != 200 || out["acked"] != float64(2) {
		t.Fatalf("ack: %d %v", status, out)
	}
	if status, out := h.call(x.dave, x.ID, "POST", "/v1/teams/"+x.Hex()+"/acks", map[string]any{"ids": []string{hx(ids[2])}}); status != 200 || out["acked"] != float64(0) {
		t.Errorf("another device's ack: %d %v", status, out)
	}
	s4 := h.stream(x.bob, x.Team, lastEventID)
	if n := countEnvelopes(s4.drain()); n != 2 {
		t.Errorf("after acking two: %d", n)
	}
	if status, _ := h.call(x.bob, x.ID, "POST", "/v1/teams/"+x.Hex()+"/acks", map[string]any{"ids": []string{"zz"}}); status != 400 {
		t.Errorf("bad id: %d", status)
	}
	// With every addressee's ack the envelope is gone from storage.
	h.call(x.bob, x.ID, "POST", "/v1/teams/"+x.Hex()+"/acks", map[string]any{"ids": []string{hx(ids[2]), hx(ids[3])}})
	if n, err := h.st.StoredBytes(x.ID); err != nil || n != 0 {
		t.Errorf("stored after every ack: %d %v", n, err)
	}
}

func countEnvelopes(es []event) int {
	n := 0
	for _, e := range es {
		if e.Name == "envelope" {
			n++
		}
	}
	return n
}

func TestEnvelopeKeptSevenDaysOrUntilAllAck(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	// Addressed to two devices: one ack is not enough.
	id := linktest.Filler(16)
	h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID, x.dave.ID}, Seq: 1, ID: id}))
	h.call(x.bob, x.ID, "POST", "/v1/teams/"+x.Hex()+"/acks", map[string]any{"ids": []string{hx(id)}})
	if n := countEnvelopes(h.stream(x.dave, x.Team, "").drain()); n != 1 {
		t.Errorf("still waiting for dave: %d", n)
	}
	if n := countEnvelopes(h.stream(x.bob, x.Team, "").drain()); n != 0 {
		t.Errorf("bob already acked: %d", n)
	}
	h.advance(7*day - time.Minute)
	if err := h.srv.Sweep(); err != nil {
		t.Fatal(err)
	}
	if n := countEnvelopes(h.stream(x.dave, x.Team, "").drain()); n != 1 {
		t.Errorf("kept almost seven days: %d", n)
	}
	h.advance(2 * time.Minute)
	if err := h.srv.Sweep(); err != nil {
		t.Fatal(err)
	}
	if n := countEnvelopes(h.stream(x.dave, x.Team, "").drain()); n != 0 {
		t.Errorf("kept past seven days: %d", n)
	}
	if n, _ := h.st.StoredBytes(x.ID); n != 0 {
		t.Errorf("stored after expiry: %d", n)
	}
}

func TestEphemeralKeepsOnlyTheLastPerDeviceAndKind(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	live := h.stream(x.bob, x.Team, "")
	live.drain()
	send := func(from *linktest.Device, kind string, to []string, ts int64) (int, map[string]any) {
		return h.send(x, from, h.envelope(x, linktest.Env{From: from, Kind: kind, To: to, TS: ts}))
	}
	for i := int64(1); i <= 3; i++ {
		if status, out := send(x.alice, "presence", nil, NOW+i); status != 201 {
			t.Fatalf("presence %d: %d %v", i, status, out)
		}
	}
	// A late older presence does not replace the newer one; it is accepted and dropped.
	if status, out := send(x.alice, "presence", nil, NOW+1); status != 200 || out["stale"] != true {
		t.Errorf("older presence: %d %v", status, out)
	}
	// A presence of another device, and a unit-status (another kind) of dave to bob.
	send(x.bob, "presence", nil, NOW+5)
	send(x.dave, "unit-status", []string{x.bob.ID}, NOW+6)
	// A stream that wakes late sees only the newest of a burst (the older ones were already replaced), so the count is not fixed: it sees alice's
	// presence (the last one among them), dave's unit-status, and never bob's own presence.
	seen := map[string]int{}
	for _, e := range live.drain() {
		if e.Name == "envelope" {
			seen[e.Data["from"].(string)]++
		}
	}
	if seen[x.alice.ID] < 1 || seen[x.alice.ID] > 3 || seen[x.dave.ID] != 1 || seen[x.bob.ID] != 0 {
		t.Errorf("live stream saw %v, want alice 1 to 3, dave 1, bob 0", seen)
	}
	// A device that connects later gets the last of each device and kind, once, and none of its own.
	late := h.stream(x.bob, x.Team, "")
	var from []string
	for _, e := range late.drain() {
		if e.Name == "envelope" {
			if e.Data["ephemeral"] != true {
				t.Errorf("only ephemerals are held: %v", e.Data)
			}
			from = append(from, e.Data["from"].(string))
		}
	}
	if len(from) != 2 || !contains(from, x.alice.ID) || !contains(from, x.dave.ID) {
		t.Errorf("late connection saw %v, want alice's presence and dave's unit-status", from)
	}
	// Dave was not addressed by the unit-status: he sees alice's and bob's presences only.
	n := 0
	for _, e := range h.stream(x.dave, x.Team, "").drain() {
		if e.Name == "envelope" {
			n++
		}
	}
	if n != 2 {
		t.Errorf("dave saw %d ephemerals, want 2", n)
	}
	// Acks never matter for them, and they are never queued: acking changes nothing.
	if n := countEnvelopes(h.stream(x.bob, x.Team, "").drain()); n != 2 {
		t.Errorf("again: %d", n)
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func TestCaps(t *testing.T) {
	t.Run("one envelope over 256 KB", func(t *testing.T) {
		h := newHarness(t)
		x := h.team3()
		ok := h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, CT: linktest.Filler(200 * 1024)})
		if status, out := h.send(x, x.alice, ok); status != 201 {
			t.Fatalf("200 KB: %d %v", status, out)
		}
		big := h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 2, CT: linktest.Filler(256 * 1024)})
		if status, out := h.send(x, x.alice, big); status != 413 || out["error"] != "envelope-too-large" {
			t.Errorf("over 256 KB: %d %v", status, out)
		}
	})
	t.Run("a request body far over the limit", func(t *testing.T) {
		h := newHarness(t)
		x := h.team3()
		raw := []byte(`{"envelope":"` + strings.Repeat("A", 2<<20) + `"}`)
		if status, _ := h.callRaw(x.alice, x.ID, "POST", "/v1/teams/"+x.Hex()+"/envelopes", raw, nil); status != 413 {
			t.Errorf("2 MB body: %d", status)
		}
	})
	t.Run("stored bytes per team", func(t *testing.T) {
		h := newHarness(t, func(o *server.Options) { o.MaxTeamBytes = 8_000 })
		x := h.team3()
		sent := 0
		var last int
		for i := 1; i <= 10; i++ {
			status, out := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: int64(i), CT: linktest.Filler(3000)}))
			last = status
			if status == 429 {
				if out["error"] != "team-full" {
					t.Errorf("reason: %v", out)
				}
				break
			}
			sent++
		}
		if last != 429 || sent != 2 {
			t.Errorf("sent %d before the cap, last status %d", sent, last)
		}
		// An ack frees space.
		s := h.stream(x.bob, x.Team, "")
		var ids []string
		for _, e := range s.drain() {
			if e.Name == "envelope" {
				ids = append(ids, e.Data["id"].(string))
			}
		}
		h.call(x.bob, x.ID, "POST", "/v1/teams/"+x.Hex()+"/acks", map[string]any{"ids": ids})
		if status, _ := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 11, CT: linktest.Filler(3000)})); status != 201 {
			t.Errorf("after acks: %d", status)
		}
		// Another team is not affected by this team's cap.
		y := h.team3()
		if status, _ := h.send(y, y.alice, h.envelope(y, linktest.Env{From: y.alice, Kind: "tower", To: []string{y.bob.ID}, Seq: 1, CT: linktest.Filler(3000)})); status != 201 {
			t.Errorf("another team: %d", status)
		}
	})
	t.Run("envelopes a minute", func(t *testing.T) {
		h := newHarness(t, func(o *server.Options) { o.RatePerMinute = 5 })
		x := h.team3()
		post := func(i int) int {
			status, _ := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: int64(i)}))
			return status
		}
		for i := 1; i <= 5; i++ {
			if s := post(i); s != 201 {
				t.Fatalf("envelope %d: %d", i, s)
			}
		}
		if s := post(6); s != 429 {
			t.Errorf("sixth in a minute: %d", s)
		}
		// Presences count too, and other teams have their own budget.
		y := h.team3()
		if status, _ := h.send(y, y.alice, h.envelope(y, linktest.Env{From: y.alice, Kind: "tower", To: []string{y.bob.ID}, Seq: 1})); status != 201 {
			t.Errorf("another team: %d", status)
		}
		h.advance(61 * time.Second)
		if s := post(7); s != 201 {
			t.Errorf("after a minute: %d", s)
		}
	})
	t.Run("many envelopes at the default rate", func(t *testing.T) {
		h := newHarness(t)
		x := h.team3()
		var first429 int
		for i := 1; i <= 650; i++ {
			status, _ := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "presence", TS: NOW + int64(i)}))
			if status == 429 && first429 == 0 {
				first429 = i
			}
		}
		if first429 != 601 {
			t.Errorf("first 429 at %d, want 601", first429)
		}
	})
}

func TestSequenceOfEventIDs(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	s := h.stream(x.bob, x.Team, "")
	for i := 1; i <= 3; i++ {
		h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: int64(i)}))
	}
	var prev int64
	for _, e := range s.drain() {
		id, err := strconv.ParseInt(e.ID, 10, 64)
		if err != nil || id <= prev {
			t.Fatalf("event ids must increase: %q after %d", e.ID, prev)
		}
		prev = id
	}
}

// Resuming keeps the cursor for everything but queued envelopes still waiting for this device; those always come again until acknowledged,
// and nothing else repeats.
func TestResumeReplaysOnlyUnackedQueuedEnvelopes(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	id := linktest.Filler(16)
	h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1, ID: id}))
	h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "presence", TS: NOW + 1}))
	first := h.stream(x.bob, x.Team, "")
	var last string
	for _, e := range first.drain() {
		last = e.ID
	}
	// A roster entry after the cursor is new; the tower envelope before it is repeated; the roster entries and the presence before are not.
	carol := linktest.NewDevice(3)
	h.mustPost(x.alice, x.Team, x.Add(x.alice, carol, true))
	got := h.stream(x.bob, x.Team, last).drain()
	var names []string
	for _, e := range got {
		names = append(names, e.Name)
	}
	if len(got) != 2 || got[0].Name != "envelope" || got[0].Data["id"] != hx(id) || got[1].Name != "roster" {
		t.Errorf("resumed events %v, want the unacked envelope then the new roster entry", names)
	}
	h.call(x.bob, x.ID, "POST", "/v1/teams/"+x.Hex()+"/acks", map[string]any{"ids": []string{hx(id)}})
	got = h.stream(x.bob, x.Team, last).drain()
	if len(got) != 1 || got[0].Name != "roster" {
		t.Errorf("after the ack: %v", got)
	}
}
