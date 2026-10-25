package server

import (
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"

	"github.com/wayne930242/straw-relay/internal/link"
	"github.com/wayne930242/straw-relay/internal/store"
)

const (
	smallBody = 64 << 10
	// An invite for an unattended device may last up to 24 hours.
	maxInviteMS = 24 * 3600_000
	// A join request is a key record, a label, a MAC, and an AEAD wrapper: a few hundred bytes.
	maxJoinRequest = 4096
)

// createTeam: POST /v1/teams {team, token, entry}. The entry has to be a valid create entry, the request signed by its founder, and the
// operator's token unspent. A refused entry or request leaves the token alone.
func (s *Server) createTeam(c *call) error {
	var in struct{ Team, Token, Entry string }
	raw, err := c.jsonBody(smallBody, &in)
	if err != nil {
		return err
	}
	team, err := decodeHex(in.Team, 16)
	if err != nil {
		return fail(http.StatusBadRequest, "bad-team")
	}
	c.team = in.Team
	entryRaw, err := decode64(in.Entry)
	if err != nil {
		return err
	}
	h, err := c.signedHeaders()
	if err != nil {
		return err
	}
	e, derr := link.DecodeEntry(entryRaw)
	if derr != nil {
		return fail(http.StatusUnprocessableEntity, link.ReasonMalformed)
	}
	st, reason := link.Apply(team, nil, e)
	if reason != "" {
		return &apiError{status: http.StatusUnprocessableEntity, code: reason, extra: map[string]any{"seq": e.Seq}}
	}
	if h.device != e.By {
		return fail(http.StatusUnauthorized, "bad-signature")
	}
	if err := c.verify(h, team, e.Subject.Record.SignPub, raw); err != nil {
		return err
	}
	switch err := s.st.CreateTeam(team, in.Token, e.Hash(), entryRaw, s.now()); {
	case errors.Is(err, store.ErrBadToken):
		return fail(http.StatusUnauthorized, "bad-token")
	case errors.Is(err, store.ErrTeamExists):
		return fail(http.StatusConflict, "team-exists")
	case err != nil:
		return err
	}
	s.cacheTeam(team, st)
	c.writeJSON(http.StatusCreated, map[string]any{"team": in.Team, "head": headJSON(st.Head)})
	return nil
}

// getRoster: GET /v1/teams/{team}/roster?after=seq returns the entries after seq (all when absent) and the head.
func (s *Server) getRoster(c *call) error {
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
	after := int64(-1)
	if v := c.r.URL.Query().Get("after"); v != "" {
		if after, err = strconv.ParseInt(v, 10, 64); err != nil || after < -1 {
			return fail(http.StatusBadRequest, "bad-after")
		}
	}
	rows, err := s.st.Roster(team, after)
	if err != nil {
		return err
	}
	entries := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		entries = append(entries, map[string]any{"seq": r.Seq, "hash": hex.EncodeToString(r.Hash), "entry": b64.EncodeToString(r.Entry)})
	}
	c.writeJSON(http.StatusOK, map[string]any{"head": headJSON(a.entry.current().Head), "entries": entries})
	return nil
}

// postRoster: POST /v1/teams/{team}/roster {entry} appends an entry by compare and set on prev. A moved head is 409 with the head; an entry the
// rules refuse is 422 with the reason.
func (s *Server) postRoster(c *call) error {
	team, err := c.pathTeam()
	if err != nil {
		return err
	}
	var in struct{ Entry string }
	raw, err := c.jsonBody(smallBody, &in)
	if err != nil {
		return err
	}
	a, err := c.authenticate(team, raw)
	if err != nil {
		return err
	}
	entryRaw, err := decode64(in.Entry)
	if err != nil {
		return err
	}
	e, derr := link.DecodeEntry(entryRaw)
	if derr != nil {
		return fail(http.StatusUnprocessableEntity, link.ReasonMalformed)
	}
	// The request is signed by the entry's signer: an entry is posted by whoever made it.
	if e.By != a.device {
		return fail(http.StatusForbidden, "not-signer")
	}
	t := a.entry
	t.mu.Lock()
	defer t.mu.Unlock()
	head := t.state.Head
	if e.Seq != head.Seq+1 || string(e.Prev) != string(head.Hash) {
		return &apiError{status: http.StatusConflict, code: "conflict", extra: map[string]any{"head": headJSON(head)}}
	}
	next, reason := link.Apply(team, t.state, e)
	if reason != "" {
		return &apiError{status: http.StatusUnprocessableEntity, code: reason, extra: map[string]any{"seq": e.Seq}}
	}
	removed := ""
	if e.Op == "remove" {
		removed = e.Subject.Record.ID
	}
	switch err := s.st.AppendRoster(team, e.Seq, e.Hash(), entryRaw, removed); {
	case errors.Is(err, store.ErrConflict):
		return &apiError{status: http.StatusConflict, code: "conflict", extra: map[string]any{"head": headJSON(head)}}
	case err != nil:
		return err
	}
	// The state moves before streams are woken, so a removed device's stream sees that it is no longer a member.
	t.state = next
	s.hub.notify(c.team)
	c.writeJSON(http.StatusCreated, map[string]any{"seq": e.Seq, "hash": hex.EncodeToString(e.Hash())})
	return nil
}

// postInvite: POST /v1/invites {team, h, expires} holds sha256 of an invite secret until `expires` (ms), for an attended member.
func (s *Server) postInvite(c *call) error {
	var in struct {
		Team, H string
		Expires int64
	}
	raw, err := c.jsonBody(smallBody, &in)
	if err != nil {
		return err
	}
	team, err := decodeHex(in.Team, 16)
	if err != nil {
		return fail(http.StatusBadRequest, "bad-team")
	}
	c.team = in.Team
	hash, err := decodeHex(in.H, 32)
	if err != nil {
		return err
	}
	a, err := c.authenticate(team, raw)
	if err != nil {
		return err
	}
	// Only an attended member signs an add, so only one may invite.
	if !a.state.Members[a.device].Attended {
		return fail(http.StatusForbidden, "not-attended")
	}
	now := s.now()
	if in.Expires <= now || in.Expires > now+maxInviteMS {
		return fail(http.StatusUnprocessableEntity, "bad-expiry")
	}
	switch err := s.st.PutInvite(team, hash, a.device, in.Expires, now, s.opt.MaxOpenInvites); {
	case errors.Is(err, store.ErrInviteExists):
		return fail(http.StatusConflict, "invite-exists")
	case errors.Is(err, store.ErrTooManyInvites):
		return fail(http.StatusTooManyRequests, "too-many-invites")
	case err != nil:
		return err
	}
	c.writeJSON(http.StatusCreated, map[string]any{"expires": in.Expires})
	return nil
}

// postJoin: POST /v1/invites/{h}/join {request} takes the invite's one join request and passes it to the inviter's stream. It carries no
// signature: the joiner is not a member yet, and the request is ciphertext under a secret the relay never sees.
func (s *Server) postJoin(c *call) error {
	hash, err := decodeHex(c.r.PathValue("h"), 32)
	if err != nil {
		return err
	}
	var in struct{ Request string }
	if _, err := c.jsonBody(maxJoinRequest*2, &in); err != nil {
		return err
	}
	blob, err := decode64(in.Request)
	if err != nil {
		return err
	}
	if len(blob) == 0 {
		return fail(http.StatusBadRequest, "empty-request")
	}
	if len(blob) > maxJoinRequest {
		return fail(http.StatusRequestEntityTooLarge, "request-too-large")
	}
	team, err := s.st.Join(hash, blob, s.now())
	switch {
	case errors.Is(err, store.ErrNoInvite):
		return fail(http.StatusNotFound, "no-invite")
	case errors.Is(err, store.ErrInviteUsed):
		return fail(http.StatusConflict, "invite-used")
	case err != nil:
		return err
	}
	c.team = hex.EncodeToString(team)
	s.hub.notify(c.team)
	c.writeJSON(http.StatusAccepted, map[string]any{"queued": true})
	return nil
}
