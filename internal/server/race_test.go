package server_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wayne930242/straw-relay/internal/linktest"
	"github.com/wayne930242/straw-relay/internal/server"
)

// gate blocks the first request of one device between its authentication and its handler, until release is called.
type gate struct {
	reached chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func newGate(h *harness, device string) *gate {
	g := &gate{reached: make(chan struct{}), proceed: make(chan struct{})}
	h.srv.SetAuthHook(func(d string) {
		if d != device {
			return
		}
		first := false
		g.once.Do(func() { first = true })
		if first {
			close(g.reached)
			<-g.proceed
		}
	})
	return g
}

func (g *gate) waitReached(t *testing.T) {
	t.Helper()
	select {
	case <-g.reached:
	case <-time.After(3 * time.Second):
		t.Fatal("the request never reached the gate")
	}
}

// A remove that commits between a request's authentication and its write must not let the removed device's envelope or invite land after the
// removal's cleanup.
func TestRemovalDuringAnEnvelopePostLeavesNothingBehind(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	g := newGate(h, x.bob.ID)
	env := h.envelope(x, linktest.Env{From: x.bob, Kind: "tower", To: []string{x.dave.ID}, Seq: 1})

	type result struct {
		status int
		out    map[string]any
	}
	done := make(chan result, 1)
	go func() {
		status, out := h.send(x, x.bob, env)
		done <- result{status, out}
	}()
	g.waitReached(t)
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	close(g.proceed)
	r := <-done

	if r.status != 401 {
		t.Errorf("the removed sender's envelope: %d %v, want 401", r.status, r.out)
	}
	if n := h.pending(x.ID, x.dave.ID); n != 0 {
		t.Errorf("dave has %d envelopes from a device removed before they were stored", n)
	}
	if n := h.stored(x.ID); n != 0 {
		t.Errorf("%d bytes stored for a removed sender", n)
	}
}

func TestRemovalDuringAnInvitePostLeavesNothingBehind(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	g := newGate(h, x.bob.ID)
	hash := hx(sha([]byte("bob's invite")))

	done := make(chan int, 1)
	go func() {
		status, _ := h.call(x.bob, x.ID, "POST", "/v1/invites", map[string]any{"team": x.Hex(), "h": hash, "expires": NOW + 600_000})
		done <- status
	}()
	g.waitReached(t)
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	close(g.proceed)
	if status := <-done; status != 401 {
		t.Errorf("the removed inviter's invite: %d, want 401", status)
	}
	if status, _ := h.callRaw(nil, nil, "POST", "/v1/invites/"+hash+"/join", []byte(`{"request":"`+b64(linktest.Filler(100))+`"}`), nil); status != 404 {
		t.Errorf("the invite exists after its inviter was removed: %d", status)
	}
}

func TestRosterAppendsRaceToOneWinner(t *testing.T) {
	h := newHarness(t)
	for round := 0; round < 15; round++ {
		x := h.team3()
		carol, eve := linktest.NewDevice(3), linktest.NewDevice(5)
		seq, hash := x.Seq, append([]byte{}, x.Hash...)
		first := x.Add(x.alice, carol, true)
		x.Seq, x.Hash = seq, hash
		second := x.Add(x.alice, eve, true)

		start := make(chan struct{})
		var wg sync.WaitGroup
		statuses := make([]int, 2)
		for i, entry := range [][]byte{first, second} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				statuses[i], _ = h.post(x.alice, x.Team, entry)
			}()
		}
		close(start)
		wg.Wait()
		if !(statuses[0] == 201 && statuses[1] == 409) && !(statuses[0] == 409 && statuses[1] == 201) {
			t.Fatalf("round %d: statuses %v, want one 201 and one 409", round, statuses)
		}
		_, out := h.call(x.alice, x.ID, "GET", "/v1/teams/"+x.Hex()+"/roster", nil)
		if entries := out["entries"].([]any); len(entries) != 4 {
			t.Fatalf("round %d: %d entries, want 4", round, len(entries))
		}
	}
}

// A read of the roster shows a head that is the last entry it returned, even while entries are being appended.
func TestRosterReadHeadMatchesItsEntries(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			select {
			case <-stop:
				return
			default:
			}
			dev := linktest.NewDevice(byte(20 + i))
			h.post(x.alice, x.Team, x.Add(x.alice, dev, true))
		}
	}()
	var bad atomic.Int32
	for i := 0; i < 150; i++ {
		_, out := h.call(x.bob, x.ID, "GET", "/v1/teams/"+x.Hex()+"/roster", nil)
		entries := out["entries"].([]any)
		last := entries[len(entries)-1].(map[string]any)
		head := out["head"].(map[string]any)
		if head["seq"] != last["seq"] || head["hash"] != last["hash"] {
			bad.Add(1)
		}
	}
	close(stop)
	wg.Wait()
	if n := bad.Load(); n > 0 {
		t.Errorf("%d reads showed a head that was not their last entry", n)
	}
}

func TestRemovalDropsQueuedMailBothWays(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	toBob := linktest.Filler(16)
	fromBob := linktest.Filler(16)
	keep := linktest.Filler(16)
	for _, c := range []struct {
		from *linktest.Device
		to   *linktest.Device
		id   []byte
		seq  int64
	}{{x.dave, x.bob, toBob, 1}, {x.bob, x.dave, fromBob, 1}, {x.dave, x.alice, keep, 2}} {
		kind := "tower"
		if c.from == x.dave {
			kind = "notice"
		}
		if status, out := h.send(x, c.from, h.envelope(x, linktest.Env{From: c.from, Kind: kind, To: []string{c.to.ID}, Seq: c.seq, ID: c.id})); status != 201 {
			t.Fatalf("send: %d %v", status, out)
		}
	}
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	if n := h.pending(x.ID, x.bob.ID); n != 0 {
		t.Errorf("bob still has %d queued envelopes", n)
	}
	if n := h.pending(x.ID, x.dave.ID); n != 0 {
		t.Errorf("dave still has %d envelopes bob sent", n)
	}
	var ids []any
	for _, e := range h.stream(x.alice, x.Team, "").drain() {
		if e.Name == "envelope" {
			ids = append(ids, e.Data["id"])
		}
	}
	if len(ids) != 1 || ids[0] != hx(keep) {
		t.Errorf("alice should keep exactly dave's envelope to her, saw %v", ids)
	}
}

func rawServer(t *testing.T, h *harness, hook func()) *httptest.Server {
	t.Helper()
	srv := server.New(h.st, server.Options{Now: func() time.Time { return time.UnixMilli(h.now.Load()) }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), KeepAlive: time.Hour})
	srv.SetLoadHook(hook)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { srv.Close(); ts.CloseClientConnections(); ts.Close() })
	return ts
}

func getRoster(t *testing.T, ts *httptest.Server, dev *linktest.Device, team []byte, hexID string, now int64) int {
	t.Helper()
	req, _ := http.NewRequest("GET", ts.URL+"/v1/teams/"+hexID+"/roster", nil)
	linktest.SignRequest(req, dev, team, now, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// A roster is loaded from the database once however many requests arrive together, and unknown teams are not looked up again at once.
func TestRosterLoadsAreSingleFlightAndNegativelyCached(t *testing.T) {
	h := newHarness(t)
	x := h.team3()

	var loads atomic.Int32
	ts := rawServer(t, h, func() { loads.Add(1); time.Sleep(30 * time.Millisecond) })
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s := getRoster(t, ts, x.bob, x.ID, x.Hex(), h.now.Load()); s != 200 {
				t.Errorf("status %d", s)
			}
		}()
	}
	wg.Wait()
	if n := loads.Load(); n != 1 {
		t.Errorf("a cold team loaded %d times by 20 simultaneous requests, want 1", n)
	}

	loads.Store(0)
	ghost := linktest.NewTeam()
	for i := 0; i < 10; i++ {
		if s := getRoster(t, ts, x.bob, ghost.ID, ghost.Hex(), h.now.Load()); s != 401 {
			t.Errorf("unknown team: %d", s)
		}
	}
	if n := loads.Load(); n != 1 {
		t.Errorf("an unknown team was looked up %d times in 10 requests, want 1", n)
	}
	// Creating the team makes it visible at once: the negative answer does not outlive it.
	tok := h.token()
	late := linktest.NewDevice(7)
	entry := ghost.Create(late)
	body := []byte(`{"team":"` + ghost.Hex() + `","token":"` + tok + `","entry":"` + b64(entry) + `"}`)
	req, _ := http.NewRequest("POST", ts.URL+"/v1/teams", bytesReader(body))
	linktest.SignRequest(req, late, ghost.ID, h.now.Load(), body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 201 {
		t.Fatalf("create: %v %v", resp, err)
	}
	resp.Body.Close()
	if s := getRoster(t, ts, late, ghost.ID, ghost.Hex(), h.now.Load()); s != 200 {
		t.Errorf("a team created after a miss: %d", s)
	}
}
