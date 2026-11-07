package server

import (
	"encoding/hex"
	"errors"
	"net/http"
	"sync"

	"github.com/wayne930242/straw-relay/internal/link"
	"github.com/wayne930242/straw-relay/internal/store"
)

const maxAckIDs = 500

// limiter keeps a sliding minute of envelope posts for each team.
type limiter struct {
	mu     sync.Mutex
	window map[string][]int64
}

func (l *limiter) allow(team string, now int64, max int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.window[team]
	i := 0
	for i < len(w) && w[i] <= now-60_000 {
		i++
	}
	w = w[i:]
	if len(w) >= max {
		l.window[team] = w
		return false
	}
	l.window[team] = append(w, now)
	return true
}

func (l *limiter) prune(now int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for team, w := range l.window {
		if len(w) == 0 || w[len(w)-1] <= now-60_000 {
			delete(l.window, team)
		}
	}
}

// deliverLive writes a live envelope to the open streams of its one addressee, a member that is not the sender, and answers how many streams that
// was. It stores nothing, so there is no duplicate and no stale: a post that reaches nobody is still a 201 with `delivered: 0`. The caller holds the
// team's lock, so a removal either lands first (the addressee is no longer a member here) or after the envelope was queued (and drops it).
func (s *Server) deliverLive(c *call, sender string, state *link.State, env *link.Envelope, raw []byte) error {
	h := &env.Header
	if len(h.To) != 1 || h.To[0] == sender {
		return fail(http.StatusUnprocessableEntity, "live-addressee")
	}
	if _, member := state.Members[h.To[0]]; !member {
		return fail(http.StatusUnprocessableEntity, "live-addressee")
	}
	c.delivered = s.live.deliver(c.team, h.To[0], liveItem{id: h.ID, from: h.From, body: raw})
	c.writeJSON(http.StatusCreated, map[string]any{"id": hex.EncodeToString(h.ID), "delivered": c.delivered})
	return nil
}

// postEnvelope: POST /v1/teams/{team}/envelopes {envelope}. The relay checks what plaintext decides (see link.CheckEnvelope), that the sender is
// the signer of the request, and the caps, then stores the envelope for its addressees (queued) or as the last of its sender and kind
// (ephemeral, never queued).
func (s *Server) postEnvelope(c *call) error {
	team, err := c.pathTeam()
	if err != nil {
		return err
	}
	var in struct{ Envelope string }
	// base64 inflates by a third; this limit leaves room for the JSON around an envelope at the cap.
	raw, err := c.jsonBody(int64(s.opt.MaxEnvelope)*2+4096, &in)
	if err != nil {
		return err
	}
	a, err := c.authenticate(team, raw)
	if err != nil {
		return err
	}
	// The envelope is decoded before the rate limit is chosen: a live kind spends the live limiter and none of the queued budget; everything
	// else, a malformed or oversized post included, spends the queued budget as before. The decode errors are returned after the limit.
	envRaw, encErr := decode64(in.Envelope)
	var env *link.Envelope
	var decErr error
	if encErr == nil {
		if len(envRaw) > s.opt.MaxEnvelope {
			decErr = fail(http.StatusRequestEntityTooLarge, "envelope-too-large")
		} else if e, derr := link.DecodeEnvelope(envRaw); derr != nil {
			decErr = fail(http.StatusBadRequest, "malformed")
		} else {
			env = e
		}
	}
	if env != nil && link.Kinds[env.Header.Kind].Live {
		c.live, c.addressees = true, len(env.Header.To)
		if !s.liveLimiter.allow(c.team, a.device, int64(len(envRaw)), s.now(), s.opt.LiveRatePerSender, s.opt.LiveBytesPerSender, s.opt.LiveRatePerTeam) {
			return fail(http.StatusTooManyRequests, "rate-limited")
		}
	} else if !s.limiter.allow(c.team, s.now(), s.opt.RatePerMinute) {
		return fail(http.StatusTooManyRequests, "rate-limited")
	}
	if encErr != nil {
		return encErr
	}
	if decErr != nil {
		return decErr
	}
	h := &env.Header
	if h.From != a.device {
		return fail(http.StatusForbidden, "from-mismatch")
	}
	// Everything that depends on the roster is decided against its state at the moment of the write, under the lock a roster append takes: a
	// remove that commits after authentication either lands first (and this is refused) or after the envelope is stored (and its cleanup drops it).
	t := a.entry
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.state
	if _, member := state.Members[a.device]; !member {
		return fail(http.StatusUnauthorized, "not-member")
	}
	if reason := link.CheckEnvelope(team, state, env, s.now()); reason != "" {
		return fail(http.StatusUnprocessableEntity, reason)
	}
	if link.Kinds[h.Kind].Live {
		return s.deliverLive(c, a.device, state, env, envRaw)
	}
	// The addressees: the named members, or every member for the whole team; never the sender itself.
	var recipients []string
	seen := map[string]bool{a.device: true}
	if len(h.To) == 0 {
		for id := range state.Members {
			if !seen[id] {
				recipients = append(recipients, id)
			}
		}
	} else {
		for _, id := range h.To {
			if _, member := state.Members[id]; member && !seen[id] {
				seen[id] = true
				recipients = append(recipients, id)
			}
		}
		if len(recipients) == 0 {
			return fail(http.StatusUnprocessableEntity, "no-addressee")
		}
	}
	if len(recipients) == 0 && !h.Ephemeral {
		return fail(http.StatusUnprocessableEntity, "no-addressee")
	}
	// An ephemeral envelope is kept under the members it reaches (the whole team stays empty), so a list that differs only in order, repeats, or
	// ids that are not members is the same key; a sender holds at most one row per member and one for the whole team.
	var addressed []string
	if len(h.To) > 0 {
		addressed = recipients
	}
	res, err := s.st.PutEnvelope(team, store.Envelope{ID: h.ID, Sender: h.From, Kind: h.Kind, Ephemeral: h.Ephemeral, Addressed: addressed, Body: envRaw, TS: h.TS, MaxKeys: len(state.Members) + 1},
		recipients, s.now()+s.opt.Retention.Milliseconds(), s.opt.MaxTeamBytes)
	switch {
	case errors.Is(err, store.ErrFull):
		return fail(http.StatusTooManyRequests, "team-full")
	case err != nil:
		return err
	}
	out := map[string]any{"id": hex.EncodeToString(h.ID), "duplicate": res.Duplicate}
	status := http.StatusCreated
	if res.Duplicate || res.Stale {
		status = http.StatusOK
	}
	if res.Stale {
		out["stale"] = true
	}
	if !res.Duplicate && !res.Stale {
		s.hub.notify(c.team)
	}
	c.writeJSON(status, out)
	return nil
}

// postAcks: POST /v1/teams/{team}/acks {ids} marks queued envelopes read by this device; an envelope every addressee has read is dropped.
func (s *Server) postAcks(c *call) error {
	team, err := c.pathTeam()
	if err != nil {
		return err
	}
	var in struct{ IDs []string }
	raw, err := c.jsonBody(smallBody*2, &in)
	if err != nil {
		return err
	}
	a, err := c.authenticate(team, raw)
	if err != nil {
		return err
	}
	if len(in.IDs) > maxAckIDs {
		return fail(http.StatusBadRequest, "too-many-ids")
	}
	ids := make([][]byte, 0, len(in.IDs))
	for _, s := range in.IDs {
		id, err := decodeHex(s, 16)
		if err != nil {
			return err
		}
		ids = append(ids, id)
	}
	n, err := s.st.Ack(team, a.device, ids)
	if err != nil {
		return err
	}
	c.writeJSON(http.StatusOK, map[string]any{"acked": n})
	return nil
}
