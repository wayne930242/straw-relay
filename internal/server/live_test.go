package server_test

import (
	"database/sql"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/wayne930242/straw-relay/internal/linktest"
	"github.com/wayne930242/straw-relay/internal/server"
)

// operateTo is an operate envelope from `from` to the given ids at ts (zero is the harness clock).
func operateTo(h *harness, x *team3, from *linktest.Device, to []string, ts int64) []byte {
	return h.envelope(x, linktest.Env{From: from, Kind: "operate", To: to, TS: ts, CT: linktest.Filler(200)})
}

// gateLive holds every stream's delivery of live envelopes until the returned function is called, so a test can queue envelopes behind a stream that
// is not writing. The test's cleanup opens the gate too, so a failed test does not leave a stream stuck.
func gateLive(t *testing.T, h *harness) func() {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	h.srv.SetLiveDrainHook(func(string) { <-gate })
	t.Cleanup(open)
	return open
}

// nextLive reads the stream up to its next live event, past the roster entries a new stream starts with.
func nextLive(s *stream) event {
	s.t.Helper()
	for {
		e, ok := s.next()
		if !ok {
			s.t.Fatal("stream ended, wanted a live event")
		}
		if e.Name == "live" {
			return e
		}
	}
}

func liveEvents(es []event) []event {
	var out []event
	for _, e := range es {
		if e.Name == "live" {
			out = append(out, e)
		}
	}
	return out
}

// rowsOf counts the stored events of a kind, reading the relay's database file beside the server.
func rowsOf(t *testing.T, path, kind string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE kind = ?`, kind).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A live envelope is never written to SQLite, answers {id, delivered}, and reaches an open stream as an `event: live` with no `id:` line.
func TestLiveIsDeliveredNowAndNeverStored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.db")
	h := newHarnessAt(t, path)
	x := h.team3()
	bob := h.stream(x.bob, x.Team, "")
	bob.drain()
	before := h.stored(x.ID)

	env := operateTo(h, x, x.alice, []string{x.bob.ID}, 0)
	status, out := h.send(x, x.alice, env)
	if status != 201 {
		t.Fatalf("live post: %d %v", status, out)
	}
	if len(out) != 2 || out["delivered"] != float64(1) {
		t.Errorf("answer %v, want exactly {id, delivered: 1}", out)
	}
	id, _ := out["id"].(string)
	if len(id) != 32 || id != strings.ToLower(id) {
		t.Errorf("id %q is not 32 lowercase hex", id)
	}
	if n := rowsOf(t, path, "operate"); n != 0 {
		t.Errorf("%d operate rows during delivery", n)
	}
	e := bob.expect("live")
	if e.ID != "" {
		t.Errorf("a live event carries the id line %q", e.ID)
	}
	if e.Data["id"] != id || e.Data["from"] != x.alice.ID || e.Data["ephemeral"] != true || e.Data["envelope"] != b64(env) {
		t.Errorf("live data %v", e.Data)
	}
	if _, has := e.Data["dropped"]; has || len(e.Data) != 4 {
		t.Errorf("live data has fields beyond id, from, ephemeral, envelope: %v", e.Data)
	}
	if n := rowsOf(t, path, "operate"); n != 0 {
		t.Errorf("%d operate rows after delivery", n)
	}
	if after := h.stored(x.ID); after != before {
		t.Errorf("stored bytes moved from %d to %d", before, after)
	}
	if n := h.pending(x.ID, x.bob.ID); n != 0 {
		t.Errorf("pending for bob: %d", n)
	}
	// A new stream and a resumed one replay nothing live.
	if got := liveEvents(h.stream(x.bob, x.Team, "").drain()); len(got) != 0 {
		t.Errorf("a new stream replayed %d live events", len(got))
	}
	if got := liveEvents(h.stream(x.bob, x.Team, "0").drain()); len(got) != 0 {
		t.Errorf("a resumed stream replayed %d live events", len(got))
	}
}

// A live envelope moves no event id: the stored events around it follow one another.
func TestLiveTakesNoEventID(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	bob := h.stream(x.bob, x.Team, "")
	bob.drain()
	send := func(seq int64) {
		if status, out := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: seq})); status != 201 {
			t.Fatalf("tower %d: %d %v", seq, status, out)
		}
	}
	send(1)
	first := bob.expect("envelope")
	if status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, 0)); status != 201 {
		t.Fatalf("live: %d %v", status, out)
	}
	bob.expect("live")
	send(2)
	second := bob.expect("envelope")
	if second.ID == "" || first.ID == "" || second.ID != nextID(t, first.ID) {
		t.Errorf("stored event ids %q then %q, want consecutive", first.ID, second.ID)
	}
}

func nextID(t *testing.T, id string) string {
	t.Helper()
	n := 0
	for _, c := range id {
		n = n*10 + int(c-'0')
	}
	n++
	s := ""
	for ; n > 0; n /= 10 {
		s = string(rune('0'+n%10)) + s
	}
	return s
}

// With no open stream the post is accepted and delivers to nobody; the envelope is gone, not waiting. Other members never see it.
func TestLiveReachesOnlyTheAddresseesOpenStreams(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	dave := h.stream(x.dave, x.Team, "")
	dave.drain()
	status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, 0))
	if status != 201 || out["delivered"] != float64(0) {
		t.Fatalf("to an offline bob: %d %v", status, out)
	}
	dave.quiet()
	bob := h.stream(x.bob, x.Team, "")
	if got := liveEvents(bob.drain()); len(got) != 0 {
		t.Errorf("bob connected later and saw %d live events", len(got))
	}
	// Two open streams of the addressee both get it, and the answer counts them.
	bob2 := h.stream(x.bob, x.Team, "")
	bob2.drain()
	status, out = h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, 0))
	if status != 201 || out["delivered"] != float64(2) {
		t.Fatalf("to two streams: %d %v", status, out)
	}
	bob.expect("live")
	bob2.expect("live")
	dave.quiet()
}

// Exactly one addressee, a member, and not the sender; anything else is 422 and nothing is delivered.
func TestLiveNeedsExactlyOneMemberAddressee(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	bob, dave := h.stream(x.bob, x.Team, ""), h.stream(x.dave, x.Team, "")
	bob.drain()
	dave.drain()
	ghost := linktest.NewDevice(9).ID
	cases := []struct {
		name string
		to   []string
	}{
		{"the whole team", nil},
		{"two members", []string{x.bob.ID, x.dave.ID}},
		{"one member twice", []string{x.bob.ID, x.bob.ID}},
		{"a member and a stranger", []string{x.bob.ID, ghost}},
		{"a stranger", []string{ghost}},
		{"the sender", []string{x.alice.ID}},
	}
	for i, c := range cases {
		status, out := h.send(x, x.alice, operateTo(h, x, x.alice, c.to, NOW+int64(i)+1))
		if status != 422 {
			t.Errorf("%s: %d %v, want 422", c.name, status, out)
		}
	}
	bob.quiet()
	dave.quiet()
}

// The same envelope checks as an ephemeral kind, and the kind's own: nothing but a sealed, current, in-window envelope from its signer is delivered.
func TestLiveRunsTheEnvelopeChecks(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	bob := h.stream(x.bob, x.Team, "")
	bob.drain()
	notEph := false
	cases := []struct {
		name    string
		env     linktest.Env
		status  int
		reason  string
		senders *linktest.Device
	}{
		{"a bad signature", linktest.Env{From: x.alice, Signer: x.dave}, 422, "bad-signature", nil},
		{"a seq", linktest.Env{From: x.alice, Seq: 1}, 422, "bad-seq", nil},
		{"the stored flag", linktest.Env{From: x.alice, Ephemeral: &notEph}, 422, "ephemeral-mismatch", nil},
		{"a future ts", linktest.Env{From: x.alice, TS: NOW + 11*60_000}, 422, "future-ts", nil},
		{"an old ts", linktest.Env{From: x.alice, TS: NOW - 8*24*3600_000}, 422, "old-ts", nil},
		{"another epoch", linktest.Env{From: x.alice, Epoch: 7}, 422, "bad-epoch", nil},
		{"a head the roster lacks", linktest.Env{From: x.alice, HeadSeq: 99, HeadHash: linktest.Filler(32)}, 422, "roster-ahead", nil},
		{"another sender in the header", linktest.Env{From: x.dave}, 403, "from-mismatch", x.alice},
	}
	for _, c := range cases {
		c.env.Kind, c.env.To = "operate", []string{x.bob.ID}
		sender := x.alice
		if c.senders != nil {
			sender = c.senders
		}
		status, out := h.send(x, sender, h.envelope(x, c.env))
		if status != c.status || out["error"] != c.reason {
			t.Errorf("%s: %d %v, want %d %s", c.name, status, out, c.status, c.reason)
		}
	}
	// A stranger's request is refused before the envelope is looked at.
	stranger := linktest.NewDevice(8)
	if status, _ := h.send(x, stranger, operateTo(h, x, stranger, []string{x.bob.ID}, 0)); status != 401 {
		t.Errorf("a stranger: %d", status)
	}
	bob.quiet()
}

// Envelopes of one sender to one addressee are written in the order they arrived.
func TestLiveKeepsArrivalOrder(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	bob := h.stream(x.bob, x.Team, "")
	bob.drain()
	var ids []string
	for i := 0; i < 40; i++ {
		status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, NOW+int64(i)+1))
		if status != 201 || out["delivered"] != float64(1) {
			t.Fatalf("post %d: %d %v", i, status, out)
		}
		ids = append(ids, out["id"].(string))
	}
	for i, want := range ids {
		if got := bob.expect("live").Data["id"]; got != want {
			t.Fatalf("event %d is %v, want %s", i, got, want)
		}
	}
}

// A queue that overflows drops its oldest envelopes and says how many on the next one it delivers.
func TestLiveOverflowDropsTheOldestAndCountsThem(t *testing.T) {
	h := newHarness(t, func(o *server.Options) { o.LiveQueueEnvelopes = 3 })
	gate := gateLive(t, h)
	x := h.team3()
	bob := h.stream(x.bob, x.Team, "")
	var ids []string
	for i := 0; i < 5; i++ {
		status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, NOW+int64(i)+1))
		if status != 201 || out["delivered"] != float64(1) {
			t.Fatalf("post %d: %d %v", i, status, out)
		}
		ids = append(ids, out["id"].(string))
	}
	gate()
	var got []event
	for len(got) < 3 {
		got = append(got, nextLive(bob))
	}
	for i, e := range got {
		if e.Data["id"] != ids[i+2] {
			t.Errorf("event %d is %v, want the %dth post", i, e.Data["id"], i+3)
		}
	}
	if got[0].Data["dropped"] != float64(2) {
		t.Errorf("first event after the overflow: dropped %v, want 2", got[0].Data["dropped"])
	}
	for _, e := range got[1:] {
		if _, has := e.Data["dropped"]; has {
			t.Errorf("only the next event carries dropped: %v", e.Data)
		}
	}
}

func TestLiveOverflowByBytes(t *testing.T) {
	h := newHarness(t, func(o *server.Options) { o.LiveQueueBytes = 2800 })
	gate := gateLive(t, h)
	x := h.team3()
	bob := h.stream(x.bob, x.Team, "")
	var ids []string
	for i := 0; i < 4; i++ {
		env := h.envelope(x, linktest.Env{From: x.alice, Kind: "operate", To: []string{x.bob.ID}, TS: NOW + int64(i) + 1, CT: linktest.Filler(1000)})
		_, out := h.send(x, x.alice, env)
		ids = append(ids, out["id"].(string))
	}
	gate()
	first := nextLive(bob)
	// Each envelope is a little over 1,000 bytes, so two fit in 2,500 and two were dropped.
	if first.Data["id"] != ids[2] || first.Data["dropped"] != float64(2) {
		t.Errorf("first event %v, want post 3 with dropped 2", first.Data)
	}
	if second := bob.expect("live"); second.Data["id"] != ids[3] {
		t.Errorf("second event %v", second.Data)
	}
}

// Live posts have a limiter of their own, and spend none of the 600 a minute for queued traffic.
func TestLiveLimiterIsSeparate(t *testing.T) {
	t.Run("per sender", func(t *testing.T) {
		h := newHarness(t, func(o *server.Options) { o.LiveRatePerSender = 3 })
		x := h.team3()
		for i := 0; i < 3; i++ {
			if status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, NOW+int64(i)+1)); status != 201 {
				t.Fatalf("post %d: %d %v", i, status, out)
			}
		}
		if status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, NOW+10)); status != 429 || out["error"] != "rate-limited" {
			t.Errorf("fourth post: %d %v", status, out)
		}
		// Another sender has a window of its own, and a minute later alice's window has moved on.
		if status, out := h.send(x, x.bob, operateTo(h, x, x.bob, []string{x.alice.ID}, NOW+11)); status != 201 {
			t.Errorf("bob: %d %v", status, out)
		}
		h.advance(61 * time.Second)
		if status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, h.now.Load())); status != 201 {
			t.Errorf("a minute later: %d %v", status, out)
		}
	})
	t.Run("per sender bytes", func(t *testing.T) {
		h := newHarness(t, func(o *server.Options) { o.LiveBytesPerSender = 2800 })
		x := h.team3()
		var statuses []int
		for i := 0; i < 4; i++ {
			env := h.envelope(x, linktest.Env{From: x.alice, Kind: "operate", To: []string{x.bob.ID}, TS: NOW + int64(i) + 1, CT: linktest.Filler(1000)})
			status, _ := h.send(x, x.alice, env)
			statuses = append(statuses, status)
		}
		if statuses[0] != 201 || statuses[1] != 201 || statuses[2] != 429 || statuses[3] != 429 {
			t.Errorf("statuses %v, want two accepted then 429", statuses)
		}
	})
	t.Run("per team", func(t *testing.T) {
		h := newHarness(t, func(o *server.Options) { o.LiveRatePerTeam = 4 })
		x := h.team3()
		for i := 0; i < 2; i++ {
			h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, NOW+int64(i)+1))
			h.send(x, x.bob, operateTo(h, x, x.bob, []string{x.alice.ID}, NOW+int64(i)+1))
		}
		if status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, NOW+10)); status != 429 || out["error"] != "rate-limited" {
			t.Errorf("team over its live limit: %d %v", status, out)
		}
	})
	t.Run("the queued budget is not spent by live", func(t *testing.T) {
		h := newHarness(t, func(o *server.Options) { o.RatePerMinute = 2 })
		x := h.team3()
		for i := 0; i < 6; i++ {
			if status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, NOW+int64(i)+1)); status != 201 {
				t.Fatalf("live %d with a queued budget of 2: %d %v", i, status, out)
			}
		}
		for seq := int64(1); seq <= 2; seq++ {
			if status, out := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: seq})); status != 201 {
				t.Fatalf("queued %d after six live posts: %d %v", seq, status, out)
			}
		}
		if status, out := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 3})); status != 429 || out["error"] != "rate-limited" {
			t.Errorf("third queued post: %d %v", status, out)
		}
	})
	t.Run("an exhausted queued budget does not stop live", func(t *testing.T) {
		h := newHarness(t, func(o *server.Options) { o.RatePerMinute = 1 })
		x := h.team3()
		h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1}))
		if status, _ := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 2})); status != 429 {
			t.Fatalf("the queued budget should be spent: %d", status)
		}
		if status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, 0)); status != 201 {
			t.Errorf("live after an exhausted queued budget: %d %v", status, out)
		}
	})
}

// Removing a member ends its live delivery at once: what was queued for it is never written, its stream ends, and a post to it is refused.
func TestRemovedMemberGetsNoLive(t *testing.T) {
	h := newHarness(t)
	gate := gateLive(t, h)
	x := h.team3()
	bob := h.stream(x.bob, x.Team, "")
	if status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, 0)); status != 201 || out["delivered"] != float64(1) {
		t.Fatalf("live before the removal: %d %v", status, out)
	}
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	gate()
	var seen []event
	for {
		e, ok := bob.next()
		if !ok {
			break
		}
		if e.Comment == "" {
			seen = append(seen, e)
		}
	}
	if got := liveEvents(seen); len(got) != 0 {
		t.Errorf("a removed member's stream wrote %d live events", len(got))
	}
	if !bob.closed() {
		t.Error("the removed member's stream must end")
	}
	status, out := h.send(x, x.alice, operateTo(h, x, x.alice, []string{x.bob.ID}, NOW+5))
	if status != 422 {
		t.Errorf("a post to the removed member: %d %v", status, out)
	}
}

// The request log shows the team, sender, addressee count, size and status for a live post, never the envelope.
func TestLiveLogsHoldNoBody(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	bob := h.stream(x.bob, x.Team, "")
	bob.drain()
	env := operateTo(h, x, x.alice, []string{x.bob.ID}, 0)
	if status, out := h.send(x, x.alice, env); status != http.StatusCreated {
		t.Fatalf("live: %d %v", status, out)
	}
	bob.expect("live")
	logs := h.logs.String()
	for _, want := range []string{x.Hex(), x.alice.ID, "status=201", "req_bytes=", "addressees=1", "delivered=1"} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs lack %q:\n%s", want, logs)
		}
	}
	for _, secret := range []string{b64(env), b64(env)[:40], b64(env)[len(b64(env))-40:]} {
		if strings.Contains(logs, secret) {
			t.Errorf("logs hold a body: %q", secret)
		}
	}
}

// The kinds that were there before stay stored: an ephemeral one is kept as the last of its sender, a queued one waits to be acknowledged.
func TestOldKindsStayStored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.db")
	h := newHarnessAt(t, path)
	x := h.team3()
	if status, out := ephTo(t, h, x, []string{x.bob.ID}, NOW+1, 100); status != 201 {
		t.Fatalf("unit-status: %d %v", status, out)
	}
	status, out := h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1}))
	if _, has := out["delivered"]; status != 201 || out["duplicate"] != false || has {
		t.Fatalf("tower: %d %v", status, out)
	}
	if rowsOf(t, path, "unit-status") != 1 || rowsOf(t, path, "tower") != 1 {
		t.Error("the stored kinds are not in SQLite")
	}
}
