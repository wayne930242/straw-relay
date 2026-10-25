package server_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wayne930242/straw-relay/internal/linktest"
	"github.com/wayne930242/straw-relay/internal/server"
	"github.com/wayne930242/straw-relay/internal/store"
)

// NOW is the fixed clock every test starts at, in milliseconds.
const NOW int64 = 1_800_000_000_000

var (
	b64 = base64.RawURLEncoding.EncodeToString
	hx  = hex.EncodeToString
)

func sha(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type harness struct {
	t    *testing.T
	st   *store.Store
	srv  *server.Server
	http *httptest.Server
	now  atomic.Int64
	logs *syncBuffer
}

func newHarness(t *testing.T, mod ...func(*server.Options)) *harness {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "relay.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, st: st, logs: &syncBuffer{}}
	h.now.Store(NOW)
	o := server.Options{
		Now:       func() time.Time { return time.UnixMilli(h.now.Load()) },
		Logger:    slog.New(slog.NewTextHandler(h.logs, nil)),
		KeepAlive: time.Hour,
	}
	for _, m := range mod {
		m(&o)
	}
	h.srv = server.New(st, o)
	h.http = httptest.NewServer(h.srv.Handler())
	t.Cleanup(func() {
		h.srv.Close()
		h.http.CloseClientConnections()
		h.http.Close()
		st.Close()
	})
	return h
}

func (h *harness) advance(d time.Duration) { h.now.Add(d.Milliseconds()) }

// call sends a signed request with a JSON body (nil for none) and returns the status and the decoded JSON object.
func (h *harness) call(dev *linktest.Device, team []byte, method, path string, body any) (int, map[string]any) {
	h.t.Helper()
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	return h.callRaw(dev, team, method, path, raw, nil)
}

func (h *harness) callRaw(dev *linktest.Device, team []byte, method, path string, raw []byte, tweak func(*http.Request)) (int, map[string]any) {
	h.t.Helper()
	req, err := http.NewRequest(method, h.http.URL+path, bytes.NewReader(raw))
	if err != nil {
		h.t.Fatal(err)
	}
	if dev != nil {
		linktest.SignRequest(req, dev, team, h.now.Load(), raw)
	}
	if tweak != nil {
		tweak(req)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("reading the response: %v", err)
	}
	var out map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			h.t.Fatalf("response is not a JSON object: %v: %q", err, data)
		}
	}
	return resp.StatusCode, out
}

// newTeam registers a team founded by founder, with a fresh operator token.
func (h *harness) newTeam(founder *linktest.Device) *linktest.Team {
	h.t.Helper()
	tm := linktest.NewTeam()
	tok := h.token()
	entry := tm.Create(founder)
	status, out := h.call(founder, tm.ID, "POST", "/v1/teams", map[string]any{"team": tm.Hex(), "token": tok, "entry": b64(entry)})
	if status != 201 {
		h.t.Fatalf("create team: %d %v", status, out)
	}
	return tm
}

func (h *harness) post(signer *linktest.Device, tm *linktest.Team, entry []byte) (int, map[string]any) {
	h.t.Helper()
	return h.call(signer, tm.ID, "POST", "/v1/teams/"+tm.Hex()+"/roster", map[string]any{"entry": b64(entry)})
}

func (h *harness) mustPost(signer *linktest.Device, tm *linktest.Team, entry []byte) {
	h.t.Helper()
	if status, out := h.post(signer, tm, entry); status != 201 {
		h.t.Fatalf("append: %d %v", status, out)
	}
}

// team3 is alice (admin), bob (member), both attended, and dave (unattended), as the roster entries 0 to 2.
type team3 struct {
	*linktest.Team
	alice, bob, dave *linktest.Device
}

func (h *harness) team3() *team3 {
	h.t.Helper()
	x := &team3{alice: linktest.NewDevice(1), bob: linktest.NewDevice(2), dave: linktest.NewDevice(4)}
	x.Team = h.newTeam(x.alice)
	h.mustPost(x.alice, x.Team, x.Add(x.alice, x.bob, true))
	h.mustPost(x.alice, x.Team, x.Add(x.alice, x.dave, false))
	return x
}

func (h *harness) envelope(x *team3, e linktest.Env) []byte {
	e.Team = x.ID
	if e.TS == 0 {
		e.TS = h.now.Load()
	}
	if e.Epoch == 0 {
		e.Epoch = x.Epoch
	}
	if e.HeadHash == nil {
		e.HeadSeq, e.HeadHash = x.Seq, x.Hash
	}
	return linktest.Envelope(e)
}

func (h *harness) send(x *team3, sender *linktest.Device, env []byte) (int, map[string]any) {
	h.t.Helper()
	return h.call(sender, x.ID, "POST", "/v1/teams/"+x.Hex()+"/envelopes", map[string]any{"envelope": b64(env)})
}

// ---- Server-sent events ------------------------------------------------------------------------------------------------------------

type event struct {
	Name, ID string
	Data     map[string]any
	Comment  string
}

type stream struct {
	t      *testing.T
	events chan event
	body   io.Closer
	status int
	done   chan struct{}
}

func (h *harness) stream(dev *linktest.Device, tm *linktest.Team, lastID string) *stream {
	h.t.Helper()
	path := "/v1/teams/" + tm.Hex() + "/events"
	req, _ := http.NewRequest("GET", h.http.URL+path, nil)
	linktest.SignRequest(req, dev, tm.ID, h.now.Load(), nil)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	s := &stream{t: h.t, events: make(chan event, 64), body: resp.Body, status: resp.StatusCode, done: make(chan struct{})}
	h.t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != 200 {
		close(s.events)
		close(s.done)
		return s
	}
	go func() {
		defer close(s.done)
		defer close(s.events)
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 1<<20), 8<<20)
		var cur event
		var data []string
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if cur.Name != "" {
					if err := json.Unmarshal([]byte(strings.Join(data, "\n")), &cur.Data); err != nil {
						s.t.Errorf("event data is not JSON: %v", err)
					}
					s.events <- cur
				}
				cur, data = event{}, nil
			case strings.HasPrefix(line, ":"):
				s.events <- event{Comment: strings.TrimSpace(line[1:])}
			case strings.HasPrefix(line, "event: "):
				cur.Name = line[7:]
			case strings.HasPrefix(line, "id: "):
				cur.ID = line[4:]
			case strings.HasPrefix(line, "data: "):
				data = append(data, line[6:])
			}
		}
	}()
	return s
}

func (s *stream) next() (event, bool) {
	s.t.Helper()
	select {
	case e, ok := <-s.events:
		return e, ok
	case <-time.After(3 * time.Second):
		s.t.Fatal("no event within 3s")
		return event{}, false
	}
}

// expect reads the next event (not a comment) and checks its name.
func (s *stream) expect(name string) event {
	s.t.Helper()
	for {
		e, ok := s.next()
		if !ok {
			s.t.Fatalf("stream ended, wanted %q", name)
		}
		if e.Comment != "" {
			continue
		}
		if e.Name != name {
			s.t.Fatalf("got event %q %v, wanted %q", e.Name, e.Data, name)
		}
		return e
	}
}

// quiet checks that nothing but comments arrive for a short while.
func (s *stream) quiet() {
	s.t.Helper()
	deadline := time.After(150 * time.Millisecond)
	for {
		select {
		case e, ok := <-s.events:
			if ok && e.Comment == "" {
				s.t.Fatalf("unexpected event %q %v", e.Name, e.Data)
			}
			if !ok {
				return
			}
		case <-deadline:
			return
		}
	}
}

// drain reads events until a quiet moment (comments do not count) and returns the real ones.
func (s *stream) drain() []event {
	var out []event
	idle := time.NewTimer(150 * time.Millisecond)
	defer idle.Stop()
	for {
		select {
		case e, ok := <-s.events:
			if !ok {
				return out
			}
			if e.Comment == "" {
				out = append(out, e)
				idle.Reset(150 * time.Millisecond)
			}
		case <-idle.C:
			return out
		}
	}
}

func (s *stream) closed() bool {
	select {
	case <-s.done:
		return true
	case <-time.After(3 * time.Second):
		return false
	}
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

func (h *harness) token() string {
	h.t.Helper()
	tok, err := h.st.CreateToken(h.now.Load())
	if err != nil {
		h.t.Fatal(err)
	}
	return tok
}

func (h *harness) pending(team []byte, device string) int {
	h.t.Helper()
	n, err := h.st.PendingFor(team, device)
	if err != nil {
		h.t.Fatal(err)
	}
	return n
}

func (h *harness) stored(team []byte) int64 {
	h.t.Helper()
	n, err := h.st.StoredBytes(team)
	if err != nil {
		h.t.Fatal(err)
	}
	return n
}
