package server_test

import (
	"bufio"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/wayne930242/straw-relay/internal/linktest"
	"github.com/wayne930242/straw-relay/internal/server"
)

func TestStreamDeliversRosterEntriesAndResumes(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	s := h.stream(x.bob, x.Team, "")
	if s.status != 200 {
		t.Fatalf("status %d", s.status)
	}
	var lastID string
	for i := 0; i < 3; i++ {
		e := s.expect("roster")
		if e.Data["seq"] != float64(i) || e.Data["hash"] == "" || e.Data["entry"] == "" {
			t.Errorf("roster event %d: %v", i, e.Data)
		}
		lastID = e.ID
	}
	// A live roster entry arrives on the open stream.
	carol := linktest.NewDevice(3)
	h.mustPost(x.alice, x.Team, x.Add(x.alice, carol, true))
	e := s.expect("roster")
	if e.Data["seq"] != float64(3) {
		t.Errorf("live roster event: %v", e.Data)
	}
	// Resume after the third: only the fourth (and an envelope sent while offline) comes.
	h.send(x, x.alice, h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1}))
	r := h.stream(x.bob, x.Team, lastID)
	got := r.drain()
	if len(got) != 2 || got[0].Name != "roster" || got[0].Data["seq"] != float64(3) || got[1].Name != "envelope" {
		t.Errorf("resumed: %v", got)
	}
	if st := h.stream(x.bob, x.Team, "abc"); st.status != 400 {
		t.Errorf("a Last-Event-ID that is not a number: %d", st.status)
	}
	if st := h.stream(linktest.NewDevice(9), x.Team, ""); st.status != 401 {
		t.Errorf("a stranger's stream: %d", st.status)
	}
}

func TestStreamKeepAliveComment(t *testing.T) {
	h := newHarness(t, func(o *server.Options) { o.KeepAlive = 40 * time.Millisecond })
	x := h.team3()
	s := h.stream(x.bob, x.Team, "")
	deadline := time.After(2 * time.Second)
	for {
		select {
		case e := <-s.events:
			if e.Comment != "" {
				return
			}
		case <-deadline:
			t.Fatal("no keepalive comment")
		}
	}
}

func TestRemovedDeviceSeesItsRemovalThenTheStreamEnds(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	s := h.stream(x.bob, x.Team, "")
	s.drain()
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	e := s.expect("roster")
	if e.Data["seq"] != float64(3) {
		t.Errorf("removal: %v", e.Data)
	}
	if !s.closed() {
		t.Error("the removed device's stream must end")
	}
	// What was queued for it is dropped; its presence is gone.
	if n, _ := h.st.PendingFor(x.ID, x.bob.ID); n != 0 {
		t.Errorf("pending for a removed device: %d", n)
	}
}

func TestRemovalDropsTheRemovedDevicesInvitesAndPresence(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	secret := linktest.Filler(32)
	hash := sha(secret)
	if status, out := h.call(x.bob, x.ID, "POST", "/v1/invites", map[string]any{"team": x.Hex(), "h": hx(hash), "expires": NOW + 600_000}); status != 201 {
		t.Fatalf("invite: %d %v", status, out)
	}
	h.send(x, x.bob, h.envelope(x, linktest.Env{From: x.bob, Kind: "presence"}))
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	if status, _ := h.callRaw(nil, nil, "POST", "/v1/invites/"+hx(hash)+"/join", []byte(`{"request":"`+b64(linktest.Filler(100))+`"}`), nil); status != 404 {
		t.Errorf("a removed inviter's invite: %d", status)
	}
	n := 0
	for _, e := range h.stream(x.dave, x.Team, "").drain() {
		if e.Name == "envelope" {
			n++
		}
	}
	if n != 0 {
		t.Errorf("a removed device's presence is still shown: %d", n)
	}
}

func TestInvitesAndJoinRequests(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	secret := linktest.Filler(32)
	hash := hx(sha(secret))
	invite := func(dev *linktest.Device, hash string, expires int64) (int, map[string]any) {
		return h.call(dev, x.ID, "POST", "/v1/invites", map[string]any{"team": x.Hex(), "h": hash, "expires": expires})
	}
	join := func(hash string, blob []byte) (int, map[string]any) {
		return h.callRaw(nil, nil, "POST", "/v1/invites/"+hash+"/join", []byte(`{"request":"`+b64(blob)+`"}`), nil)
	}

	if status, out := invite(x.dave, hash, NOW+600_000); status != 403 || out["error"] != "not-attended" {
		t.Errorf("an unattended device invites: %d %v", status, out)
	}
	if status, _ := invite(linktest.NewDevice(9), hash, NOW+600_000); status != 401 {
		t.Errorf("a stranger invites: %d", status)
	}
	if status, out := invite(x.alice, hash, NOW-1); status != 422 || out["error"] != "bad-expiry" {
		t.Errorf("already expired: %d %v", status, out)
	}
	if status, out := invite(x.alice, hash, NOW+25*int64(time.Hour/time.Millisecond)); status != 422 || out["error"] != "bad-expiry" {
		t.Errorf("beyond 24 hours: %d %v", status, out)
	}
	if status, _ := invite(x.alice, "abcd", NOW+600_000); status != 400 {
		t.Errorf("short hash: %d", status)
	}
	// Alice is offline when the join request comes; it waits for her stream.
	if status, out := invite(x.alice, hash, NOW+600_000); status != 201 {
		t.Fatalf("invite: %d %v", status, out)
	}
	if status, out := invite(x.alice, hash, NOW+600_000); status != 409 {
		t.Errorf("the same hash twice: %d %v", status, out)
	}
	if status, _ := join(hx(sha([]byte("unknown"))), linktest.Filler(100)); status != 404 {
		t.Errorf("unknown invite: %d", status)
	}
	if status, _ := join("nothex", linktest.Filler(100)); status != 400 {
		t.Errorf("bad hash: %d", status)
	}
	if status, _ := join(hash, linktest.Filler(5000)); status != 413 && status != 400 {
		t.Errorf("a huge join request: %d", status)
	}
	blob := linktest.Filler(180)
	if status, out := join(hash, blob); status != 202 {
		t.Fatalf("join: %d %v", status, out)
	}
	// One join each.
	if status, out := join(hash, linktest.Filler(180)); status != 409 || out["error"] != "invite-used" {
		t.Errorf("second join: %d %v", status, out)
	}
	bob := h.stream(x.bob, x.Team, "")
	bob.drain()
	if got := countJoins(bob.drain()); got != 0 {
		t.Errorf("only the inviter sees a join request: %d", got)
	}
	alice := h.stream(x.alice, x.Team, "")
	var seen []event
	for _, e := range alice.drain() {
		if e.Name == "join" {
			seen = append(seen, e)
		}
	}
	if len(seen) != 1 || seen[0].Data["hash"] != hash || seen[0].Data["request"] != b64(blob) {
		t.Fatalf("join events: %v", seen)
	}
	// While her stream is open a new request is live.
	hash2 := hx(sha([]byte("second")))
	invite(x.alice, hash2, NOW+600_000)
	join(hash2, blob)
	e := alice.expect("join")
	if e.Data["hash"] != hash2 {
		t.Errorf("live join: %v", e.Data)
	}
	// Past its expiry an invite is gone.
	hash3 := hx(sha([]byte("third")))
	invite(x.alice, hash3, NOW+600_000)
	h.advance(11 * time.Minute)
	if status, _ := join(hash3, blob); status != 404 {
		t.Errorf("expired invite: %d", status)
	}
	if err := h.srv.Sweep(); err != nil {
		t.Fatal(err)
	}
	// An unattended device's invite may last a day.
	if status, out := invite(x.alice, hx(sha([]byte("long"))), h.now.Load()+23*int64(time.Hour/time.Millisecond)); status != 201 {
		t.Errorf("23 hours: %d %v", status, out)
	}
}

func countJoins(es []event) int {
	n := 0
	for _, e := range es {
		if e.Name == "join" {
			n++
		}
	}
	return n
}

func TestServerTimeoutsHoldNoIdleOrSlowConnection(t *testing.T) {
	h := newHarness(t)
	srv := server.NewHTTPServer("127.0.0.1:0", h.srv.Handler(), server.Timeouts{ReadHeader: 150 * time.Millisecond, Read: 400 * time.Millisecond, Write: 2 * time.Second, Idle: 200 * time.Millisecond})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })

	expectClosed := func(name string, c net.Conn, within time.Duration) {
		t.Helper()
		_ = c.SetReadDeadline(time.Now().Add(within))
		buf := make([]byte, 1024)
		for {
			_, err := c.Read(buf)
			if err == nil {
				continue // a 408 or a response; keep reading until the close
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Errorf("%s: the server did not close the connection within %v", name, within)
			}
			return
		}
	}

	// Slow headers: one byte, then nothing.
	c, _ := net.Dial("tcp", ln.Addr().String())
	_, _ = c.Write([]byte("GET /healthz HTTP/1.1\r\nHost: x\r\n"))
	expectClosed("slow header", c, 2*time.Second)
	c.Close()

	// A connection that sends nothing at all.
	c, _ = net.Dial("tcp", ln.Addr().String())
	expectClosed("silent connection", c, 2*time.Second)
	c.Close()

	// A body that stops half way.
	c, _ = net.Dial("tcp", ln.Addr().String())
	_, _ = c.Write([]byte("POST /v1/invites/" + strings.Repeat("a", 64) + "/join HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n{\"req"))
	expectClosed("slow body", c, 2*time.Second)
	c.Close()

	// An idle keep-alive connection after a complete request.
	c, _ = net.Dial("tcp", ln.Addr().String())
	_, _ = c.Write([]byte("GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n"))
	r := bufio.NewReader(c)
	resp, err := http.ReadResponse(r, nil)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v %v", resp, err)
	}
	resp.Body.Close()
	expectClosed("idle keep-alive", c, 2*time.Second)
	c.Close()
}

func TestLogsHoldIdsAndSizesNeverBodies(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	env := h.envelope(x, linktest.Env{From: x.alice, Kind: "tower", To: []string{x.bob.ID}, Seq: 1})
	h.send(x, x.alice, env)
	secret := b64(linktest.Filler(100))
	h.callRaw(nil, nil, "POST", "/v1/invites/"+hx(sha([]byte("x")))+"/join", []byte(`{"request":"`+secret+`"}`), nil)
	logs := h.logs.String()
	for _, want := range []string{x.Hex(), x.alice.ID, "status=201", "status=404", "POST"} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs lack %q", want)
		}
	}
	for _, secretText := range []string{b64(env), secret, b64(env)[:40]} {
		if strings.Contains(logs, secretText) {
			t.Errorf("logs hold a body: %q", secretText)
		}
	}
	if !strings.Contains(logs, "req_bytes=") {
		t.Errorf("logs lack sizes:\n%s", logs)
	}
}
