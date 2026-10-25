// Package server is the relay's HTTP API: it authenticates signed requests, applies the roster rules, routes envelopes, and streams events.
// It never decrypts anything and never logs a body.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/wayne930242/straw-relay/internal/link"
	"github.com/wayne930242/straw-relay/internal/store"
)

// Options configure a Server. Zero values take the spec's numbers.
type Options struct {
	Now    func() time.Time
	Logger *slog.Logger
	// KeepAlive is the interval of the comment that keeps proxies from closing a stream (25 s).
	KeepAlive time.Duration
	// MaxEnvelope is the largest envelope in bytes (256 KB); MaxTeamBytes is what a team may have stored (50 MB).
	MaxEnvelope  int
	MaxTeamBytes int64
	// RatePerMinute is the envelopes a team may post in a minute (600).
	RatePerMinute int
	// Retention is how long an unacknowledged envelope is kept (7 days).
	Retention time.Duration
	// MaxOpenInvites is the unexpired invites a team may hold (50).
	MaxOpenInvites int
}

func (o *Options) defaults() {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.KeepAlive == 0 {
		o.KeepAlive = 25 * time.Second
	}
	if o.MaxEnvelope == 0 {
		o.MaxEnvelope = 256 * 1024
	}
	if o.MaxTeamBytes == 0 {
		o.MaxTeamBytes = 50 << 20
	}
	if o.RatePerMinute == 0 {
		o.RatePerMinute = 600
	}
	if o.Retention == 0 {
		o.Retention = 7 * 24 * time.Hour
	}
	if o.MaxOpenInvites == 0 {
		o.MaxOpenInvites = 50
	}
}

// Server is the relay.
type Server struct {
	st   *store.Store
	opt  Options
	log  *slog.Logger
	mux  *http.ServeMux
	done chan struct{}
	once sync.Once

	teamsMu sync.Mutex
	teams   map[string]*teamEntry

	hub     hub
	limiter limiter
	streams streamSet
}

// New makes a Server over a store.
func New(st *store.Store, o Options) *Server {
	o.defaults()
	s := &Server{st: st, opt: o, log: o.Logger, mux: http.NewServeMux(), done: make(chan struct{}), teams: map[string]*teamEntry{}}
	s.hub.subs = map[string]map[chan struct{}]struct{}{}
	s.limiter.window = map[string][]int64{}
	s.streams.byDevice = map[string][]*streamHandle{}
	s.route("GET /healthz", s.healthz)
	s.route("POST /v1/teams", s.createTeam)
	s.route("GET /v1/teams/{team}/roster", s.getRoster)
	s.route("POST /v1/teams/{team}/roster", s.postRoster)
	s.route("POST /v1/teams/{team}/envelopes", s.postEnvelope)
	s.route("GET /v1/teams/{team}/events", s.events)
	s.route("POST /v1/teams/{team}/acks", s.postAcks)
	s.route("POST /v1/invites", s.postInvite)
	s.route("POST /v1/invites/{h}/join", s.postJoin)
	return s
}

// Handler is the HTTP handler to serve.
func (s *Server) Handler() http.Handler { return s.mux }

// Close ends every open stream.
func (s *Server) Close() { s.once.Do(func() { close(s.done) }) }

func (s *Server) now() int64 { return s.opt.Now().UnixMilli() }

// Sweep drops what has passed its time (envelopes kept past retention, expired invites) and stale rate windows.
func (s *Server) Sweep() error {
	s.limiter.prune(s.now())
	return s.st.Sweep(s.now())
}

// Run sweeps every minute until ctx ends.
func (s *Server) Run(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.Sweep(); err != nil {
				s.log.Error("sweep failed", "err", err)
			}
		}
	}
}

// ---- Requests and responses ----------------------------------------------------------------------------------------------------------

// call is one request in flight; what it records is what the log shows.
type call struct {
	s        *Server
	w        *recorder
	r        *http.Request
	team     string // hex, once known
	device   string
	reqBytes int
	reason   string
}

// apiError is an error the client sees: a status and a short code, plus optional fields.
type apiError struct {
	status int
	code   string
	extra  map[string]any
}

func (e *apiError) Error() string { return e.code }

func fail(status int, code string) *apiError { return &apiError{status: status, code: code} }

type recorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(p)
	r.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the connection (flushing, deadlines).
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (s *Server) route(pattern string, fn func(*call) error) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		c := &call{s: s, w: &recorder{ResponseWriter: w}, r: r}
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("handler panic", "route", pattern, "panic", p)
				if c.w.status == 0 {
					c.writeJSON(500, map[string]any{"error": "internal"})
				}
			}
			status := c.w.status
			if status == 0 {
				status = 200
			}
			// Teams, devices, sizes, and status codes only: never a body, a token, or a query string.
			s.log.Info("request", "method", r.Method, "route", pattern, "team", c.team, "device", c.device, "status", status,
				"req_bytes", c.reqBytes, "resp_bytes", c.w.bytes, "ms", time.Since(start).Milliseconds(), "reason", c.reason)
		}()
		if err := fn(c); err != nil {
			var ae *apiError
			if !errors.As(err, &ae) {
				s.log.Error("internal error", "route", pattern, "err", err)
				ae = fail(500, "internal")
			}
			c.reason = ae.code
			body := map[string]any{"error": ae.code}
			for k, v := range ae.extra {
				body[k] = v
			}
			if ae.status == 429 {
				c.w.Header().Set("Retry-After", "60")
			}
			c.writeJSON(ae.status, body)
		}
	})
}

func (c *call) writeJSON(status int, v any) {
	c.w.Header().Set("Content-Type", "application/json")
	c.w.WriteHeader(status)
	_ = json.NewEncoder(c.w).Encode(v)
}

// body reads at most limit bytes of the request body.
func (c *call) body(limit int64) ([]byte, error) {
	data, err := io.ReadAll(http.MaxBytesReader(c.w, c.r.Body, limit))
	c.reqBytes = len(data)
	if err != nil {
		var mb *http.MaxBytesError
		if errors.As(err, &mb) {
			return nil, fail(http.StatusRequestEntityTooLarge, "body-too-large")
		}
		return nil, fail(http.StatusBadRequest, "unreadable-body")
	}
	return data, nil
}

// jsonBody reads a JSON object body of at most limit bytes into v, returning the raw bytes too (a signature covers them).
func (c *call) jsonBody(limit int64, v any) ([]byte, error) {
	raw, err := c.body(limit)
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 || v != nil {
		if err := json.Unmarshal(raw, v); err != nil {
			return raw, fail(http.StatusBadRequest, "bad-json")
		}
	}
	return raw, nil
}

func (s *Server) healthz(c *call) error {
	c.writeJSON(200, map[string]any{"ok": true})
	return nil
}

var b64 = base64.RawURLEncoding.Strict()

func decode64(s string) ([]byte, error) {
	b, err := b64.DecodeString(s)
	if err != nil {
		return nil, fail(http.StatusBadRequest, "bad-encoding")
	}
	return b, nil
}

func decodeHex(s string, n int) ([]byte, error) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != n {
		return nil, fail(http.StatusBadRequest, "bad-encoding")
	}
	return b, nil
}

func hash(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

func headJSON(h link.Head) map[string]any {
	return map[string]any{"seq": h.Seq, "hash": hex.EncodeToString(h.Hash)}
}
