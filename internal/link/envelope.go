package link

import (
	"bytes"
	"fmt"

	"github.com/wayne930242/straw-relay/internal/canon"
)

// The envelope (spec, Envelope). The relay never decrypts, so it checks the header and the signature, which it can, and leaves what depends on
// a receiver's own state (addressing, the seq and ts guards, the team key) to the receivers.

// Version is the only envelope version.
const Version = 1

// Limits of the spec: an envelope may be dated 10 minutes ahead and up to 7 days back, in milliseconds.
const (
	MaxFutureMS = 10 * 60_000
	MaxAgeMS    = 7 * 24 * 3600_000
)

// Header is a decoded envelope header.
type Header struct {
	V        int64
	Team     []byte
	Epoch    int64
	HeadSeq  int64
	HeadHash []byte
	From     string
	// Seq is per sender, +1 for each queued envelope; 0 for an ephemeral one.
	Seq  int64
	ID   []byte
	TS   int64
	Kind string
	// To holds device ids; empty means the whole team.
	To        []string
	Ephemeral bool
}

// Envelope is a decoded envelope. HeaderRaw is the header as received: the AEAD's associated data and part of what the signature covers.
type Envelope struct {
	Header    Header
	HeaderRaw []byte
	Nonce     []byte
	CT        []byte
	Sig       []byte
}

func decodeHeader(b []byte) (h Header, err error) {
	f, err := canon.Dec(b, 12)
	if err != nil {
		return h, err
	}
	if len(f[1]) != 16 || len(f[4]) != 32 || len(f[7]) != 16 {
		return h, fmt.Errorf("%w: bad field length in a header", canon.ErrMalformed)
	}
	h = Header{Team: f[1], HeadHash: f[4], From: string(f[5]), ID: f[7], Kind: string(f[9])}
	if h.V, err = canon.ReadInt(f[0]); err != nil {
		return h, err
	}
	if h.Epoch, err = canon.ReadInt(f[2]); err != nil {
		return h, err
	}
	if h.HeadSeq, err = canon.ReadInt(f[3]); err != nil {
		return h, err
	}
	if h.Seq, err = canon.ReadInt(f[6]); err != nil {
		return h, err
	}
	if h.TS, err = canon.ReadInt(f[8]); err != nil {
		return h, err
	}
	to, err := canon.ReadList(f[10])
	if err != nil {
		return h, err
	}
	for _, t := range to {
		h.To = append(h.To, string(t))
	}
	h.Ephemeral, err = canon.ReadBool(f[11])
	return h, err
}

// DecodeEnvelope reads enc(header, nonce, ct, sig); any structural fault is an error wrapping canon.ErrMalformed.
func DecodeEnvelope(raw []byte) (*Envelope, error) {
	f, err := canon.Dec(raw, 4)
	if err != nil {
		return nil, err
	}
	if len(f[1]) != 12 || len(f[3]) != 64 {
		return nil, fmt.Errorf("%w: bad nonce or signature length", canon.ErrMalformed)
	}
	h, err := decodeHeader(f[0])
	if err != nil {
		return nil, err
	}
	return &Envelope{Header: h, HeaderRaw: f[0], Nonce: f[1], CT: f[2], Sig: f[3]}, nil
}

// Reasons an envelope is refused, as the reference names them.
const (
	EnvBadHeader      = "bad-header"
	EnvBadVersion     = "bad-version"
	EnvWrongTeam      = "wrong-team"
	EnvUnknownKind    = "unknown-kind"
	EnvEphemeralMis   = "ephemeral-mismatch"
	EnvNotMember      = "not-member"
	EnvKindNotAllowed = "kind-not-allowed"
	EnvBadEpoch       = "bad-epoch"
	EnvWholeTeam      = "whole-team-not-allowed"
	EnvBadSeq         = "bad-seq"
	EnvRosterAhead    = "roster-ahead"
	EnvRosterMismatch = "roster-mismatch"
	EnvFutureTS       = "future-ts"
	EnvOldTS          = "old-ts"
	EnvBadSignature   = "bad-signature"
)

// CheckEnvelope runs the checks of the spec's Envelope list that need only the roster, in the receiver's order: the header's shape, the
// sender's membership, the signature, then the kind, epoch, whole-team, seq shape, roster head, and ts checks. It returns "" when all pass.
// `now` is in milliseconds. Checks against a receiver's own state (addressing, last seq or ts taken, the key) are not here.
func CheckEnvelope(team []byte, st *State, e *Envelope, now int64) string {
	h := &e.Header
	if h.V != Version {
		return EnvBadVersion
	}
	if !bytes.Equal(h.Team, team) {
		return EnvWrongTeam
	}
	rule, ok := Kinds[h.Kind]
	if !ok {
		return EnvUnknownKind
	}
	if h.Ephemeral != rule.Ephemeral {
		return EnvEphemeralMis
	}
	if !IDOK(h.From) {
		return EnvBadHeader
	}
	for _, id := range h.To {
		if !IDOK(id) {
			return EnvBadHeader
		}
	}
	sender, ok := st.Members[h.From]
	if !ok {
		return EnvNotMember
	}
	if !VerifySig(sender.Record.SignPub, SignedBytes(ContextEnvelope, team, e.HeaderRaw, e.Nonce, e.CT), e.Sig) {
		return EnvBadSignature
	}
	if rule.Sender != SenderAny && (rule.Sender == SenderAttended) != sender.Attended {
		return EnvKindNotAllowed
	}
	// The current epoch, or the one before it when the sender wrote before it knew of the rotation.
	if h.Epoch != st.Epoch && !(h.Epoch == st.Epoch-1 && h.HeadSeq < st.EpochStart) {
		return EnvBadEpoch
	}
	if len(h.To) == 0 && !rule.WholeTeam {
		return EnvWholeTeam
	}
	// A queued kind's first seq is 1 (the guard is "greater than the last taken", from 0); an ephemeral kind has none.
	if (rule.Ephemeral && h.Seq != 0) || (!rule.Ephemeral && h.Seq < 1) {
		return EnvBadSeq
	}
	if h.HeadSeq > st.Head.Seq {
		return EnvRosterAhead
	}
	if !bytes.Equal(st.Hashes[h.HeadSeq], h.HeadHash) {
		return EnvRosterMismatch
	}
	if h.TS > now+MaxFutureMS {
		return EnvFutureTS
	}
	if h.TS < now-MaxAgeMS {
		return EnvOldTS
	}
	return ""
}
