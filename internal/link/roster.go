package link

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/wayne930242/straw-relay/internal/canon"
)

// The roster (spec, Roster): an append-only log of signed entries. This file checks every rule that plaintext decides: signers and their
// authority, `attended`, roles, the id ban, epochs, and the set a key is sealed to. What needs a device's own keys (opening a sealed key,
// reading a label table) belongs to the clients.

// Roles.
const (
	RoleAdmin  = "admin"
	RoleMember = "member"
)

// FirstEpoch is the first team key's epoch.
const FirstEpoch = 1

// Reasons an entry is refused. They are the reference implementation's, so a client and the relay name a refusal the same way.
const (
	ReasonMalformed         = "malformed"
	ReasonBadSeq            = "bad-seq"
	ReasonBadPrev           = "bad-prev"
	ReasonBadOp             = "bad-op"
	ReasonBadFounder        = "bad-founder"
	ReasonSignerNotMember   = "signer-not-member"
	ReasonBadSignature      = "bad-signature"
	ReasonSignerUnattended  = "signer-unattended"
	ReasonNotAdmin          = "not-admin"
	ReasonBadEpoch          = "bad-epoch"
	ReasonBadSubject        = "bad-subject"
	ReasonBadRole           = "bad-role"
	ReasonBanned            = "banned"
	ReasonDuplicateMember   = "duplicate-member"
	ReasonUnknownMember     = "unknown-member"
	ReasonUnattendedAdmin   = "unattended-admin"
	ReasonNoAttendedAdmin   = "no-attended-admin"
	ReasonBadPayload        = "bad-payload"
	ReasonSealedSetMismatch = "sealed-set-mismatch"
)

// minBlob is a nonce and a tag: the smallest thing a team key can seal.
const minBlob = 12 + 16

// Subject is a device as an entry names it.
type Subject struct {
	Record   KeyRecord
	Role     string
	Attended bool
}

// Entry is a decoded roster entry. Raw is its encoding as received, which its hash covers; Body is what its signature covers.
type Entry struct {
	Seq     int64
	Prev    []byte
	Op      string
	Subject *Subject // nil for rotate
	By      string
	Epoch   int64
	Payload []byte
	Sig     []byte
	Raw     []byte
	Body    []byte
}

// Hash is sha256 of the full encoding, signature included.
func (e *Entry) Hash() []byte {
	sum := sha256.Sum256(e.Raw)
	return sum[:]
}

// DecodeEntry reads an entry; any structural fault is an error wrapping canon.ErrMalformed.
func DecodeEntry(raw []byte) (*Entry, error) {
	f, err := canon.Dec(raw, 8)
	if err != nil {
		return nil, err
	}
	e := &Entry{Op: string(f[2]), By: string(f[4]), Payload: f[6], Prev: f[1], Sig: f[7], Raw: raw}
	switch e.Op {
	case "create", "add", "remove", "role", "rotate":
	default:
		return nil, fmt.Errorf("%w: unknown op", canon.ErrMalformed)
	}
	if len(e.Prev) != 32 || len(e.Sig) != 64 {
		return nil, fmt.Errorf("%w: bad prev or signature length", canon.ErrMalformed)
	}
	if e.Seq, err = canon.ReadInt(f[0]); err != nil {
		return nil, err
	}
	if e.Epoch, err = canon.ReadInt(f[5]); err != nil {
		return nil, err
	}
	if len(f[3]) > 0 {
		s, err := canon.Dec(f[3], 3)
		if err != nil {
			return nil, err
		}
		sub := &Subject{Role: string(s[1])}
		if sub.Role != RoleAdmin && sub.Role != RoleMember {
			return nil, fmt.Errorf("%w: unknown role", canon.ErrMalformed)
		}
		if sub.Record, err = DecodeKeyRecord(s[0]); err != nil {
			return nil, err
		}
		if sub.Attended, err = canon.ReadBool(s[2]); err != nil {
			return nil, err
		}
		e.Subject = sub
	}
	// The signature is the last field, 4 + 64 bytes; the body is every field before it, as received.
	e.Body = raw[:len(raw)-68]
	return e, nil
}

// Member is a device that is in the roster.
type Member struct {
	Subject
	// Since is the seq of the entry that made the device a member.
	Since int64
}

// Head is the newest entry's seq and hash.
type Head struct {
	Seq  int64
	Hash []byte
}

// State is a roster after some entries. Apply returns a new State and never changes the one it is given.
type State struct {
	Epoch int64
	// EpochStart is the seq of the entry that started the current epoch.
	EpochStart int64
	Head       Head
	// Hashes holds the hash of each entry, by seq.
	Hashes  [][]byte
	Members map[string]Member
	// Banned ids can never be added again.
	Banned map[string]bool
}

func (s *State) clone() *State {
	c := *s
	c.Hashes = append([][]byte(nil), s.Hashes...)
	c.Members = make(map[string]Member, len(s.Members))
	for k, v := range s.Members {
		c.Members[k] = v
	}
	c.Banned = make(map[string]bool, len(s.Banned))
	for k := range s.Banned {
		c.Banned[k] = true
	}
	return &c
}

func attendedAdmins(m map[string]Member) int {
	n := 0
	for _, x := range m {
		if x.Role == RoleAdmin && x.Attended {
			n++
		}
	}
	return n
}

// sealedList reads the payload of a remove or rotate: a list of (id, sealed key) and the label table. Nil when it is not that, or an id repeats.
func sealedList(payload []byte) (ids map[string]bool, table []byte) {
	f, err := canon.Dec(payload, 2)
	if err != nil {
		return nil, nil
	}
	items, err := canon.ReadList(f[0])
	if err != nil {
		return nil, nil
	}
	ids = map[string]bool{}
	for _, it := range items {
		p, err := canon.Dec(it, 2)
		if err != nil || ids[string(p[0])] {
			return nil, nil
		}
		ids[string(p[0])] = true
	}
	return ids, f[1]
}

// Apply appends one entry to a roster (nil before create) and returns the new state, or the reason it is refused.
func Apply(team []byte, prior *State, e *Entry) (*State, string) {
	sub := e.Subject
	if prior == nil {
		if e.Seq != 0 {
			return nil, ReasonBadSeq
		}
		if e.Op != "create" || !bytes.Equal(e.Prev, make([]byte, 32)) {
			return nil, ReasonBadOp
		}
		if sub == nil || sub.Role != RoleAdmin || !sub.Attended || e.By != sub.Record.ID || e.Epoch != FirstEpoch {
			return nil, ReasonBadFounder
		}
		if !VerifySig(sub.Record.SignPub, SignedBytes(ContextRoster, team, e.Body), e.Sig) {
			return nil, ReasonBadSignature
		}
		if len(e.Payload) < minBlob {
			return nil, ReasonBadPayload
		}
		h := e.Hash()
		return &State{
			Epoch: FirstEpoch, Head: Head{0, h}, Hashes: [][]byte{h},
			Members: map[string]Member{sub.Record.ID: {Subject: *sub}},
			Banned:  map[string]bool{},
		}, ""
	}
	if e.Seq != prior.Head.Seq+1 {
		return nil, ReasonBadSeq
	}
	if !bytes.Equal(e.Prev, prior.Head.Hash) {
		return nil, ReasonBadPrev
	}
	if e.Op == "create" {
		return nil, ReasonBadOp
	}
	signer, ok := prior.Members[e.By]
	if !ok {
		return nil, ReasonSignerNotMember
	}
	if !VerifySig(signer.Record.SignPub, SignedBytes(ContextRoster, team, e.Body), e.Sig) {
		return nil, ReasonBadSignature
	}
	if !signer.Attended {
		return nil, ReasonSignerUnattended
	}
	if e.Op != "add" && signer.Role != RoleAdmin {
		return nil, ReasonNotAdmin
	}

	next := prior.clone()
	finish := func() (*State, string) {
		h := e.Hash()
		next.Hashes = append(next.Hashes, h)
		next.Head = Head{e.Seq, h}
		return next, ""
	}
	// remove and rotate: a new key sealed to exactly the devices that remain, and the whole label table under it.
	reKey := func() string {
		if e.Epoch != prior.Epoch+1 {
			return ReasonBadEpoch
		}
		ids, table := sealedList(e.Payload)
		if ids == nil || len(table) < minBlob {
			return ReasonBadPayload
		}
		if len(ids) != len(next.Members) {
			return ReasonSealedSetMismatch
		}
		for id := range next.Members {
			if !ids[id] {
				return ReasonSealedSetMismatch
			}
		}
		next.Epoch, next.EpochStart = e.Epoch, e.Seq
		return ""
	}

	switch e.Op {
	case "add":
		if sub == nil {
			return nil, ReasonBadSubject
		}
		if e.Epoch != prior.Epoch {
			return nil, ReasonBadEpoch
		}
		if sub.Role != RoleMember {
			return nil, ReasonBadRole
		}
		if prior.Banned[sub.Record.ID] {
			return nil, ReasonBanned
		}
		if _, dup := prior.Members[sub.Record.ID]; dup {
			return nil, ReasonDuplicateMember
		}
		parts, err := canon.Dec(e.Payload, 2)
		if err != nil || len(parts[0]) < minBlob {
			return nil, ReasonBadPayload
		}
		next.Members[sub.Record.ID] = Member{Subject: *sub, Since: e.Seq}
		return finish()
	case "remove":
		if sub == nil {
			return nil, ReasonBadSubject
		}
		m, ok := prior.Members[sub.Record.ID]
		if !ok {
			return nil, ReasonUnknownMember
		}
		if !sameRecord(sub.Record, m.Record) || sub.Role != m.Role || sub.Attended != m.Attended {
			return nil, ReasonBadSubject
		}
		delete(next.Members, sub.Record.ID)
		if attendedAdmins(next.Members) == 0 {
			return nil, ReasonNoAttendedAdmin
		}
		next.Banned[sub.Record.ID] = true
		if r := reKey(); r != "" {
			return nil, r
		}
		return finish()
	case "role":
		if sub == nil {
			return nil, ReasonBadSubject
		}
		m, ok := prior.Members[sub.Record.ID]
		if !ok {
			return nil, ReasonUnknownMember
		}
		if !sameRecord(sub.Record, m.Record) || sub.Attended != m.Attended {
			return nil, ReasonBadSubject
		}
		if e.Epoch != prior.Epoch {
			return nil, ReasonBadEpoch
		}
		if p, err := canon.Dec(e.Payload, 1); err != nil || string(p[0]) != sub.Role {
			return nil, ReasonBadPayload
		}
		if sub.Role == RoleAdmin && !sub.Attended {
			return nil, ReasonUnattendedAdmin
		}
		m.Role = sub.Role
		next.Members[sub.Record.ID] = m
		if attendedAdmins(next.Members) == 0 {
			return nil, ReasonNoAttendedAdmin
		}
		return finish()
	case "rotate":
		if sub != nil {
			return nil, ReasonBadSubject
		}
		if r := reKey(); r != "" {
			return nil, r
		}
		return finish()
	}
	return nil, ReasonBadOp
}

func sameRecord(a, b KeyRecord) bool {
	return a.ID == b.ID && bytes.Equal(a.SignPub, b.SignPub) && bytes.Equal(a.BoxPub, b.BoxPub)
}

// Reject is a refused log: the reason and the entry's position.
type Reject struct {
	Reason string
	Seq    int
}

// Replay applies a whole log from create, checking every rule. A failure's Seq is the entry's position in the log.
func Replay(team []byte, raws [][]byte) (*State, *Reject) {
	if len(raws) == 0 {
		return nil, &Reject{ReasonBadSeq, 0}
	}
	var st *State
	for i, raw := range raws {
		e, err := DecodeEntry(raw)
		if err != nil {
			return nil, &Reject{ReasonMalformed, i}
		}
		next, reason := Apply(team, st, e)
		if reason != "" {
			return nil, &Reject{reason, i}
		}
		st = next
	}
	return st, nil
}
