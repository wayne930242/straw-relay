package server

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"sync"

	"github.com/wayne930242/straw-relay/internal/link"
)

// A request's signature is valid within this many ms of the relay's clock.
const requestWindowMS = 5 * 60_000

// teamEntry caches a team's roster state. mu serializes appends, so the check and the write of an entry are one step.
type teamEntry struct {
	mu    sync.Mutex
	state *link.State
}

func (t *teamEntry) current() *link.State {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// team returns the cached entry of a team, loading its roster on first use. It is nil for a team that does not exist.
func (s *Server) team(id []byte) (*teamEntry, error) {
	key := string(id)
	s.teamsMu.Lock()
	defer s.teamsMu.Unlock()
	if t, ok := s.teams[key]; ok {
		return t, nil
	}
	rows, err := s.st.Roster(id, -1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	raws := make([][]byte, len(rows))
	for i, r := range rows {
		raws[i] = r.Entry
	}
	st, rej := link.Replay(id, raws)
	if rej != nil {
		// The stored log was checked entry by entry on the way in; failing here means the database was changed behind the relay's back.
		return nil, fail(500, "stored-roster-invalid")
	}
	t := &teamEntry{state: st}
	s.teams[key] = t
	return t, nil
}

func (s *Server) cacheTeam(id []byte, st *link.State) {
	s.teamsMu.Lock()
	s.teams[string(id)] = &teamEntry{state: st}
	s.teamsMu.Unlock()
}

type signed struct {
	device string
	timeMS int64
	sig    []byte
}

// signedHeaders reads X-Straw-Device, X-Straw-Time, and X-Straw-Sig and checks the time window.
func (c *call) signedHeaders() (*signed, error) {
	h := c.r.Header
	dev, ts, sig := h.Get("X-Straw-Device"), h.Get("X-Straw-Time"), h.Get("X-Straw-Sig")
	if dev == "" || ts == "" || sig == "" || !link.IDOK(dev) {
		return nil, fail(http.StatusUnauthorized, "missing-auth")
	}
	t, err := strconv.ParseInt(ts, 10, 64)
	rawSig, herr := hex.DecodeString(sig)
	if err != nil || herr != nil || len(rawSig) != 64 {
		return nil, fail(http.StatusUnauthorized, "missing-auth")
	}
	if d := c.s.now() - t; d > requestWindowMS || d < -requestWindowMS {
		return nil, fail(http.StatusUnauthorized, "bad-time")
	}
	c.device = dev
	return &signed{dev, t, rawSig}, nil
}

// verify checks the signature over the received bytes: method, the request target as sent (path with query), time, and the body's hash.
func (c *call) verify(h *signed, team, pub, body []byte) error {
	uri := c.r.RequestURI
	if len(uri) == 0 || uri[0] != '/' {
		return fail(http.StatusBadRequest, "bad-target")
	}
	if !link.VerifySig(pub, link.RequestBytes(team, c.r.Method, uri, h.timeMS, body), h.sig) {
		return fail(http.StatusUnauthorized, "bad-signature")
	}
	return nil
}

// authed is a request from a current member of a team.
type authed struct {
	device string
	team   []byte
	entry  *teamEntry
	state  *link.State
}

// authenticate accepts a request only from a current, non-removed member of the team, within the time window, over the bytes received. An
// unknown team answers like a non-member, so an outsider learns nothing about which teams exist.
func (c *call) authenticate(team, body []byte) (*authed, error) {
	h, err := c.signedHeaders()
	if err != nil {
		return nil, err
	}
	t, err := c.s.team(team)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, fail(http.StatusUnauthorized, "not-member")
	}
	st := t.current()
	m, ok := st.Members[h.device]
	if !ok {
		return nil, fail(http.StatusUnauthorized, "not-member")
	}
	if err := c.verify(h, team, m.Record.SignPub, body); err != nil {
		return nil, err
	}
	return &authed{device: h.device, team: team, entry: t, state: st}, nil
}

// pathTeam reads {team} from the path and records it for the log.
func (c *call) pathTeam() ([]byte, error) {
	team, err := decodeHex(c.r.PathValue("team"), 16)
	if err != nil {
		return nil, fail(http.StatusBadRequest, "bad-team")
	}
	c.team = c.r.PathValue("team")
	return team, nil
}
