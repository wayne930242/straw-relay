// Package store keeps the relay's state in SQLite (modernc.org/sqlite, no cgo): operator tokens, rosters, the event log that streams read, queued
// envelopes with their pending addressees, and invites. Every method is one transaction.
package store

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	_ "modernc.org/sqlite"
)

// Errors a caller tells apart.
var (
	ErrBadToken       = errors.New("operator token missing, unknown, or already used")
	ErrTeamExists     = errors.New("team exists")
	ErrConflict       = errors.New("roster head moved")
	ErrFull           = errors.New("team storage cap reached")
	ErrInviteExists   = errors.New("invite hash exists")
	ErrTooManyInvites = errors.New("too many open invites")
	ErrNoInvite       = errors.New("no such invite")
	ErrInviteUsed     = errors.New("invite already used")
)

// Event types.
const (
	TypeRoster   = "roster"
	TypeEnvelope = "envelope"
	TypeJoin     = "join"
)

const schema = `
CREATE TABLE IF NOT EXISTS tokens (hash BLOB PRIMARY KEY, created_at INTEGER NOT NULL, used_at INTEGER, team BLOB);
CREATE TABLE IF NOT EXISTS teams (id BLOB PRIMARY KEY, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS roster (team BLOB NOT NULL, seq INTEGER NOT NULL, hash BLOB NOT NULL, entry BLOB NOT NULL, PRIMARY KEY (team, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS events (
	eid INTEGER PRIMARY KEY AUTOINCREMENT,
	team BLOB NOT NULL,
	typ TEXT NOT NULL,
	ref BLOB,
	seq INTEGER,
	sender TEXT NOT NULL DEFAULT '',
	kind TEXT NOT NULL DEFAULT '',
	ephemeral INTEGER NOT NULL DEFAULT 0,
	addressed TEXT NOT NULL DEFAULT '[]',
	body BLOB NOT NULL,
	size INTEGER NOT NULL,
	ts INTEGER NOT NULL DEFAULT 0,
	expires_at INTEGER
);
CREATE INDEX IF NOT EXISTS events_team ON events (team, eid);
CREATE UNIQUE INDEX IF NOT EXISTS events_envelope_ref ON events (team, ref) WHERE typ = 'envelope';
CREATE INDEX IF NOT EXISTS events_ephemeral ON events (team, sender, kind) WHERE ephemeral = 1;
CREATE INDEX IF NOT EXISTS events_expiry ON events (expires_at) WHERE expires_at IS NOT NULL;
CREATE TABLE IF NOT EXISTS deliveries (eid INTEGER NOT NULL REFERENCES events (eid) ON DELETE CASCADE, device TEXT NOT NULL, PRIMARY KEY (eid, device)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS deliveries_device ON deliveries (device);
CREATE TABLE IF NOT EXISTS invites (hash BLOB PRIMARY KEY, team BLOB NOT NULL, inviter TEXT NOT NULL, expires_at INTEGER NOT NULL, used INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS invites_team ON invites (team);
`

// Store is the relay's database.
type Store struct{ db *sql.DB }

// Open opens (and creates) the database at path. Writes take the lock up front, so concurrent writers wait instead of failing.
func Open(path string) (*Store, error) {
	q := url.Values{}
	for _, p := range []string{"busy_timeout(5000)", "journal_mode(WAL)", "synchronous(NORMAL)", "foreign_keys(1)"} {
		q.Add("_pragma", p)
	}
	q.Set("_txlock", "immediate")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) tx(fn func(*sql.Tx) error) error {
	t, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := fn(t); err != nil {
		_ = t.Rollback()
		return err
	}
	return t.Commit()
}

// ---- Tokens and teams ----------------------------------------------------------------------------------------------------------------

func tokenHash(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// CreateToken makes a one-time team token and returns it; only its hash is stored.
func (s *Store) CreateToken(now int64) (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := "srt1_" + base64.RawURLEncoding.EncodeToString(raw)
	_, err := s.db.Exec(`INSERT INTO tokens (hash, created_at) VALUES (?, ?)`, tokenHash(token), now)
	return token, err
}

// CreateTeam registers a team with its create entry, spending the token in the same transaction. A token that is unknown or used is
// ErrBadToken; a team id that exists is ErrTeamExists and leaves the token unspent.
func (s *Store) CreateTeam(team []byte, token string, hash, entry []byte, now int64) error {
	return s.tx(func(t *sql.Tx) error {
		var used sql.NullInt64
		if err := t.QueryRow(`SELECT used_at FROM tokens WHERE hash = ?`, tokenHash(token)).Scan(&used); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrBadToken
			}
			return err
		}
		if used.Valid {
			return ErrBadToken
		}
		var n int
		if err := t.QueryRow(`SELECT COUNT(*) FROM teams WHERE id = ?`, team).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return ErrTeamExists
		}
		if _, err := t.Exec(`INSERT INTO teams (id, created_at) VALUES (?, ?)`, team, now); err != nil {
			return err
		}
		if err := insertRoster(t, team, 0, hash, entry); err != nil {
			return err
		}
		_, err := t.Exec(`UPDATE tokens SET used_at = ?, team = ? WHERE hash = ?`, now, team, tokenHash(token))
		return err
	})
}

func insertRoster(t *sql.Tx, team []byte, seq int64, hash, entry []byte) error {
	if _, err := t.Exec(`INSERT INTO roster (team, seq, hash, entry) VALUES (?, ?, ?, ?)`, team, seq, hash, entry); err != nil {
		return err
	}
	_, err := t.Exec(`INSERT INTO events (team, typ, seq, body, size) VALUES (?, 'roster', ?, ?, ?)`, team, seq, entry, len(entry))
	return err
}

// RosterRow is one stored entry.
type RosterRow struct {
	Seq   int64
	Hash  []byte
	Entry []byte
}

// Roster returns the entries with seq greater than after (-1 for all), in order.
func (s *Store) Roster(team []byte, after int64) ([]RosterRow, error) {
	rows, err := s.db.Query(`SELECT seq, hash, entry FROM roster WHERE team = ? AND seq > ? ORDER BY seq`, team, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RosterRow
	for rows.Next() {
		var r RosterRow
		if err := rows.Scan(&r.Seq, &r.Hash, &r.Entry); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AppendRoster stores the entry with this seq when it directly follows the stored head (ErrConflict otherwise). When it removes a device,
// the device's queued mail, presence, join requests, and open invites go with it.
func (s *Store) AppendRoster(team []byte, seq int64, hash, entry []byte, removed string) error {
	return s.tx(func(t *sql.Tx) error {
		var head sql.NullInt64
		if err := t.QueryRow(`SELECT MAX(seq) FROM roster WHERE team = ?`, team).Scan(&head); err != nil {
			return err
		}
		if !head.Valid || head.Int64+1 != seq {
			return ErrConflict
		}
		if err := insertRoster(t, team, seq, hash, entry); err != nil {
			return err
		}
		if removed == "" {
			return nil
		}
		quoted := `%"` + removed + `"%`
		for _, q := range []struct {
			sql  string
			args []any
		}{
			{`DELETE FROM deliveries WHERE device = ? AND eid IN (SELECT eid FROM events WHERE team = ?)`, []any{removed, team}},
			{`DELETE FROM events WHERE team = ? AND typ = 'envelope' AND sender = ?`, []any{team, removed}},
			{`DELETE FROM events WHERE team = ? AND typ = 'envelope' AND ephemeral = 0 AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.eid = events.eid)`, []any{team}},
			{`DELETE FROM events WHERE team = ? AND typ = 'join' AND addressed LIKE ?`, []any{team, quoted}},
			{`DELETE FROM invites WHERE team = ? AND inviter = ?`, []any{team, removed}},
		} {
			if _, err := t.Exec(q.sql, q.args...); err != nil {
				return err
			}
		}
		return nil
	})
}

// ---- Envelopes -----------------------------------------------------------------------------------------------------------------------

// Envelope is what the relay stores of one: its bytes and routing.
type Envelope struct {
	ID        []byte
	Sender    string
	Kind      string
	Ephemeral bool
	// Addressed is the header's `to`; empty means the whole team (ephemeral kinds only are read through it).
	Addressed []string
	Body      []byte
	TS        int64
}

// PutResult says what PutEnvelope did.
type PutResult struct {
	// Duplicate: the envelope id was already stored.
	Duplicate bool
	// Stale: an ephemeral envelope no newer than the one kept for its sender and kind.
	Stale bool
}

// PutEnvelope stores an envelope. A queued one gets a pending delivery for each of recipients; an ephemeral one replaces the last of its
// sender and kind and is never queued. It counts against maxBytes of stored envelope bytes in the team (ErrFull).
func (s *Store) PutEnvelope(team []byte, e Envelope, recipients []string, expires, maxBytes int64) (PutResult, error) {
	var res PutResult
	err := s.tx(func(t *sql.Tx) error {
		var n int
		if err := t.QueryRow(`SELECT COUNT(*) FROM events WHERE team = ? AND typ = 'envelope' AND ref = ?`, team, e.ID).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			res.Duplicate = true
			return nil
		}
		var replaced, replacedSize int64
		if e.Ephemeral {
			var oldTS int64
			err := t.QueryRow(`SELECT eid, ts, size FROM events WHERE team = ? AND typ = 'envelope' AND ephemeral = 1 AND sender = ? AND kind = ?`, team, e.Sender, e.Kind).Scan(&replaced, &oldTS, &replacedSize)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil && oldTS >= e.TS {
				res.Stale = true
				return nil
			}
		}
		var stored int64
		if err := t.QueryRow(`SELECT COALESCE(SUM(size), 0) FROM events WHERE team = ? AND typ <> 'roster'`, team).Scan(&stored); err != nil {
			return err
		}
		if stored-replacedSize+int64(len(e.Body)) > maxBytes {
			return ErrFull
		}
		if replaced != 0 {
			if _, err := t.Exec(`DELETE FROM events WHERE eid = ?`, replaced); err != nil {
				return err
			}
		}
		to, _ := json.Marshal(append([]string{}, e.Addressed...))
		r, err := t.Exec(`INSERT INTO events (team, typ, ref, sender, kind, ephemeral, addressed, body, size, ts, expires_at) VALUES (?, 'envelope', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			team, e.ID, e.Sender, e.Kind, e.Ephemeral, string(to), e.Body, len(e.Body), e.TS, expires)
		if err != nil {
			return err
		}
		eid, _ := r.LastInsertId()
		if !e.Ephemeral {
			for _, d := range recipients {
				if _, err := t.Exec(`INSERT INTO deliveries (eid, device) VALUES (?, ?)`, eid, d); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return res, err
}

// Ack marks envelopes read by a device and drops those every addressee has read. It returns how many pending deliveries it cleared.
func (s *Store) Ack(team []byte, device string, ids [][]byte) (int, error) {
	cleared := 0
	err := s.tx(func(t *sql.Tx) error {
		for _, id := range ids {
			var eid int64
			if err := t.QueryRow(`SELECT eid FROM events WHERE team = ? AND typ = 'envelope' AND ephemeral = 0 AND ref = ?`, team, id).Scan(&eid); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return err
			}
			r, err := t.Exec(`DELETE FROM deliveries WHERE eid = ? AND device = ?`, eid, device)
			if err != nil {
				return err
			}
			if n, _ := r.RowsAffected(); n == 0 {
				continue
			}
			cleared++
			if _, err := t.Exec(`DELETE FROM events WHERE eid = ? AND NOT EXISTS (SELECT 1 FROM deliveries WHERE eid = ?)`, eid, eid); err != nil {
				return err
			}
		}
		return nil
	})
	return cleared, err
}

// StoredBytes is the size of the envelopes and join requests a team has stored.
func (s *Store) StoredBytes(team []byte) (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COALESCE(SUM(size), 0) FROM events WHERE team = ? AND typ <> 'roster'`, team).Scan(&n)
	return n, err
}

// PendingFor counts the queued envelopes a device has not acknowledged.
func (s *Store) PendingFor(team []byte, device string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM deliveries d JOIN events e ON e.eid = d.eid WHERE e.team = ? AND d.device = ?`, team, device).Scan(&n)
	return n, err
}

// ---- Events --------------------------------------------------------------------------------------------------------------------------

// Event is one item of a device's stream.
type Event struct {
	EID       int64
	Type      string
	Seq       int64  // roster
	Ref       []byte // envelope id, or invite hash
	Sender    string
	Ephemeral bool
	Body      []byte
}

// EventsAfter returns the events with eid greater than after that a device may see, oldest first: every roster entry, the queued envelopes
// addressed to it and not yet acknowledged, the last ephemeral envelope of each sender and kind addressed to it (or to the whole team), and
// the join requests of its invites. With onlyRoster, only roster entries (what a removed device is still shown).
func (s *Store) EventsAfter(team []byte, device string, after int64, limit int, onlyRoster bool) ([]Event, error) {
	q := `SELECT eid, typ, COALESCE(seq, 0), ref, sender, ephemeral, body FROM events e WHERE team = ? AND eid > ? AND (typ = 'roster'`
	args := []any{team, after}
	if !onlyRoster {
		q += ` OR (typ = 'join' AND instr(addressed, ?) > 0)
			OR (typ = 'envelope' AND ephemeral = 1 AND sender <> ? AND (addressed = '[]' OR instr(addressed, ?) > 0))
			OR (typ = 'envelope' AND ephemeral = 0 AND EXISTS (SELECT 1 FROM deliveries d WHERE d.eid = e.eid AND d.device = ?))`
		quoted := `"` + device + `"`
		args = append(args, quoted, device, quoted, device)
	}
	q += `) ORDER BY eid LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.EID, &e.Type, &e.Seq, &e.Ref, &e.Sender, &e.Ephemeral, &e.Body); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- Invites -------------------------------------------------------------------------------------------------------------------------

// PutInvite holds an invite hash until it expires. A team may have maxOpen unexpired invites at a time.
func (s *Store) PutInvite(team, hash []byte, inviter string, expires, now int64, maxOpen int) error {
	return s.tx(func(t *sql.Tx) error {
		var n int
		if err := t.QueryRow(`SELECT COUNT(*) FROM invites WHERE hash = ?`, hash).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return ErrInviteExists
		}
		if err := t.QueryRow(`SELECT COUNT(*) FROM invites WHERE team = ? AND expires_at > ?`, team, now).Scan(&n); err != nil {
			return err
		}
		if n >= maxOpen {
			return ErrTooManyInvites
		}
		_, err := t.Exec(`INSERT INTO invites (hash, team, inviter, expires_at) VALUES (?, ?, ?, ?)`, hash, team, inviter, expires)
		return err
	})
}

// Join takes the one join request an invite accepts and queues it for the inviter's stream. It returns the invite's team.
func (s *Store) Join(hash, request []byte, now int64) ([]byte, error) {
	var team []byte
	err := s.tx(func(t *sql.Tx) error {
		var inviter string
		var expires int64
		var used bool
		if err := t.QueryRow(`SELECT team, inviter, expires_at, used FROM invites WHERE hash = ?`, hash).Scan(&team, &inviter, &expires, &used); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNoInvite
			}
			return err
		}
		if expires <= now {
			return ErrNoInvite
		}
		if used {
			return ErrInviteUsed
		}
		if _, err := t.Exec(`UPDATE invites SET used = 1 WHERE hash = ?`, hash); err != nil {
			return err
		}
		to, _ := json.Marshal([]string{inviter})
		_, err := t.Exec(`INSERT INTO events (team, typ, ref, addressed, body, size, expires_at) VALUES (?, 'join', ?, ?, ?, ?, ?)`, team, hash, string(to), request, len(request), expires)
		return err
	})
	return team, err
}

// ---- Housekeeping --------------------------------------------------------------------------------------------------------------------

// Sweep drops what has passed its time: envelopes kept 7 days, join requests and invites past their expiry.
func (s *Store) Sweep(now int64) error {
	return s.tx(func(t *sql.Tx) error {
		if _, err := t.Exec(`DELETE FROM events WHERE expires_at IS NOT NULL AND expires_at <= ?`, now); err != nil {
			return err
		}
		_, err := t.Exec(`DELETE FROM invites WHERE expires_at <= ?`, now)
		return err
	})
}
