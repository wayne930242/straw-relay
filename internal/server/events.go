package server

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/wayne930242/straw-relay/internal/store"
)

const (
	streamPage = 100
	// A device may hold this many streams (a link holder takeover overlaps two); the oldest ends when it opens another.
	maxStreamsPerDevice = 4
	streamWriteTimeout  = 30 * time.Second
)

// hub wakes the streams of a team when something new is stored for it.
type hub struct {
	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

func (h *hub) subscribe(team string) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	if h.subs[team] == nil {
		h.subs[team] = map[chan struct{}]struct{}{}
	}
	h.subs[team][ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs[team], ch)
		if len(h.subs[team]) == 0 {
			delete(h.subs, team)
		}
		h.mu.Unlock()
	}
}

func (h *hub) notify(team string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs[team] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

type streamHandle struct{ cancel chan struct{} }

type streamSet struct {
	mu       sync.Mutex
	byDevice map[string][]*streamHandle
}

func (ss *streamSet) add(key string) (*streamHandle, func()) {
	h := &streamHandle{cancel: make(chan struct{})}
	ss.mu.Lock()
	ss.byDevice[key] = append(ss.byDevice[key], h)
	if len(ss.byDevice[key]) > maxStreamsPerDevice {
		close(ss.byDevice[key][0].cancel)
		ss.byDevice[key] = ss.byDevice[key][1:]
	}
	ss.mu.Unlock()
	return h, func() {
		ss.mu.Lock()
		defer ss.mu.Unlock()
		list := ss.byDevice[key]
		for i, x := range list {
			if x == h {
				ss.byDevice[key] = append(list[:i:i], list[i+1:]...)
				break
			}
		}
		if len(ss.byDevice[key]) == 0 {
			delete(ss.byDevice, key)
		}
	}
}

// events: GET /v1/teams/{team}/events is a server-sent event stream for the calling device. Event names and data:
//
//	roster    {"seq", "hash", "entry"}                       every roster entry, from the start (or after Last-Event-ID)
//	envelope  {"id", "from", "ephemeral", "envelope"}        queued envelopes addressed to the device and not yet acked; the last ephemeral
//	                                                         one of each sender and kind addressed to it or to the whole team
//	join      {"hash", "request"}                            join requests for the device's invites
//
// Each event's `id:` is a number that only grows; Last-Event-ID resumes after it. A comment line keeps the connection alive. A device removed
// from the team is shown the roster entries it has not seen, its own removal included, and the stream ends.
func (s *Server) events(c *call) error {
	team, err := c.pathTeam()
	if err != nil {
		return err
	}
	body, err := c.body(1024)
	if err != nil {
		return err
	}
	a, err := c.authenticate(team, body)
	if err != nil {
		return err
	}
	cursor := int64(0)
	if v := c.r.Header.Get("Last-Event-ID"); v != "" {
		if cursor, err = strconv.ParseInt(v, 10, 64); err != nil || cursor < 0 {
			return fail(http.StatusBadRequest, "bad-last-event-id")
		}
	}
	rc := http.NewResponseController(c.w)
	// The server's ReadTimeout would otherwise cancel the request's context while the stream is still open.
	_ = rc.SetReadDeadline(time.Time{})
	w := c.w
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	write := func(format string, args ...any) error {
		_ = rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
		if _, err := fmt.Fprintf(w, format, args...); err != nil {
			return err
		}
		return rc.Flush()
	}
	if err := rc.Flush(); err != nil {
		return nil
	}

	wake, unsub := s.hub.subscribe(c.team)
	defer unsub()
	handle, remove := s.streams.add(c.team + "/" + a.device)
	defer remove()
	tick := time.NewTicker(s.opt.KeepAlive)
	defer tick.Stop()
	ctx := c.r.Context()

	first := true
	for {
		member := false
		if t, err := s.team(team); err == nil && t != nil {
			_, member = t.current().Members[a.device]
		}
		events, err := s.st.EventsAfter(team, a.device, cursor, streamPage, first, !member)
		first = false
		if err != nil {
			return nil
		}
		for _, e := range events {
			data, err := eventData(e)
			if err != nil {
				return nil
			}
			if write("id: %d\nevent: %s\ndata: %s\n\n", e.EID, e.Type, data) != nil {
				return nil
			}
			cursor = e.EID
		}
		if len(events) == streamPage {
			continue
		}
		if !member {
			return nil
		}
		select {
		case <-wake:
		case <-tick.C:
			if write(": keepalive\n\n") != nil {
				return nil
			}
		case <-ctx.Done():
			return nil
		case <-handle.cancel:
			return nil
		case <-s.done:
			return nil
		}
	}
}

func eventData(e store.Event) ([]byte, error) {
	switch e.Type {
	case store.TypeRoster:
		return json.Marshal(map[string]any{"seq": e.Seq, "hash": hex.EncodeToString(hash(e.Body)), "entry": b64.EncodeToString(e.Body)})
	case store.TypeEnvelope:
		return json.Marshal(map[string]any{"id": hex.EncodeToString(e.Ref), "from": e.Sender, "ephemeral": e.Ephemeral, "envelope": b64.EncodeToString(e.Body)})
	default:
		return json.Marshal(map[string]any{"hash": hex.EncodeToString(e.Ref), "request": b64.EncodeToString(e.Body)})
	}
}
