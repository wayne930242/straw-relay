// Package linktest builds signed Straw Link structures for tests: devices, roster entries, envelopes with filler ciphertext, and signed
// requests. The relay never opens a sealed key, a label table, or a ciphertext, so fillers stand in for them; the signatures are real.
package linktest

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"

	"github.com/wayne930242/straw-relay/internal/canon"
	"github.com/wayne930242/straw-relay/internal/link"
)

// Device is a test device: an Ed25519 key, and a fixed filler for its encryption key.
type Device struct {
	ID      string
	SignPub []byte
	BoxPub  []byte
	Priv    ed25519.PrivateKey
}

// FromSeed makes the device whose signing key comes from a 32-byte seed.
func FromSeed(seed []byte) *Device {
	priv := ed25519.NewKeyFromSeed(seed)
	pub := []byte(priv.Public().(ed25519.PublicKey))
	box := sha256.Sum256(append([]byte("box/"), seed...))
	return &Device{ID: link.DeviceID(pub), SignPub: pub, BoxPub: box[:], Priv: priv}
}

// NewDevice is a device whose seed is 32 copies of n.
func NewDevice(n byte) *Device {
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = n
	}
	return FromSeed(seed)
}

// Record is the device's key record: enc(id, signing public key, encryption public key).
func (d *Device) Record() []byte { return canon.Enc(canon.Str(d.ID), d.SignPub, d.BoxPub) }

// Filler is n bytes that look like ciphertext to a relay that cannot open it.
func Filler(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// Subj is the device an entry is about.
type Subj struct {
	Dev      *Device
	Role     string
	Attended bool
}

// EntryParams are the fields of a roster entry, all of them under the caller's control, so tests can build entries the rules refuse.
type EntryParams struct {
	Seq     int64
	Prev    []byte
	Op      string
	Subject *Subj
	By      string // the signer's id when empty
	Epoch   int64
	Payload []byte
}

// Entry is the signed encoding of an entry.
func Entry(team []byte, signer *Device, p EntryParams) []byte {
	by := p.By
	if by == "" {
		by = signer.ID
	}
	subject := []byte{}
	if p.Subject != nil {
		subject = canon.Enc(p.Subject.Dev.Record(), canon.Str(p.Subject.Role), canon.Bool(p.Subject.Attended))
	}
	body := []canonField{canon.Int(uint64(p.Seq)), p.Prev, canon.Str(p.Op), subject, canon.Str(by), canon.Int(uint64(p.Epoch)), p.Payload}
	enc := canon.Enc(body...)
	sig := ed25519.Sign(signer.Priv, link.SignedBytes(link.ContextRoster, team, enc))
	return canon.Enc(append(body, sig)...)
}

type canonField = []byte

// AddPayload is the payload of an add: a label under the team key and a sealed key, both fillers.
func AddPayload() []byte { return canon.Enc(Filler(40), Filler(80)) }

// RekeyPayload is the payload of a remove or rotate: a sealed key for each id, then the label table.
func RekeyPayload(ids ...string) []byte {
	var items [][]byte
	for _, id := range ids {
		items = append(items, canon.Enc(canon.Str(id), Filler(80)))
	}
	return canon.Enc(canon.List(items...), Filler(60))
}

// Team follows a roster's head as a test builds it.
type Team struct {
	ID    []byte
	Seq   int64 // -1 before create
	Hash  []byte
	Epoch int64
	// Members are the ids after the last entry built.
	Members map[string]Subj
}

// NewTeam is a team with a random id and no entries.
func NewTeam() *Team {
	return &Team{ID: Filler(16), Seq: -1, Members: map[string]Subj{}}
}

// Hex is the team id as the paths show it.
func (t *Team) Hex() string { return hex.EncodeToString(t.ID) }

func (t *Team) push(raw []byte) []byte {
	sum := sha256.Sum256(raw)
	t.Seq++
	t.Hash = sum[:]
	return raw
}

// Create makes the create entry of a founder (an attended admin).
func (t *Team) Create(founder *Device) []byte {
	t.Epoch = 1
	t.Members[founder.ID] = Subj{founder, "admin", true}
	return t.push(Entry(t.ID, founder, EntryParams{Seq: 0, Prev: make([]byte, 32), Op: "create", Subject: &Subj{founder, "admin", true}, Epoch: 1, Payload: Filler(60)}))
}

// Add makes an add of a member by signer.
func (t *Team) Add(signer, who *Device, attended bool) []byte {
	t.Members[who.ID] = Subj{who, "member", attended}
	return t.push(Entry(t.ID, signer, EntryParams{Seq: t.Seq + 1, Prev: t.Hash, Op: "add", Subject: &Subj{who, "member", attended}, Epoch: t.Epoch, Payload: AddPayload()}))
}

// Role makes a role entry by signer.
func (t *Team) Role(signer, who *Device, role string) []byte {
	m := t.Members[who.ID]
	m.Role = role
	t.Members[who.ID] = m
	return t.push(Entry(t.ID, signer, EntryParams{Seq: t.Seq + 1, Prev: t.Hash, Op: "role", Subject: &Subj{who, role, m.Attended}, Epoch: t.Epoch, Payload: canon.Enc(canon.Str(role))}))
}

// Remove makes a remove of who by signer, sealing the next epoch's key to every other member.
func (t *Team) Remove(signer, who *Device) []byte {
	m := t.Members[who.ID]
	delete(t.Members, who.ID)
	t.Epoch++
	return t.push(Entry(t.ID, signer, EntryParams{Seq: t.Seq + 1, Prev: t.Hash, Op: "remove", Subject: &Subj{who, m.Role, m.Attended}, Epoch: t.Epoch, Payload: RekeyPayload(t.ids()...)}))
}

// Rotate makes a rotate by signer.
func (t *Team) Rotate(signer *Device) []byte {
	t.Epoch++
	return t.push(Entry(t.ID, signer, EntryParams{Seq: t.Seq + 1, Prev: t.Hash, Op: "rotate", Epoch: t.Epoch, Payload: RekeyPayload(t.ids()...)}))
}

func (t *Team) ids() []string {
	var ids []string
	for id := range t.Members {
		ids = append(ids, id)
	}
	return ids
}

// Env describes an envelope; zero values are filled with ones the relay accepts.
type Env struct {
	Team      []byte
	From      *Device
	Kind      string
	To        []string
	Epoch     int64
	HeadSeq   int64
	HeadHash  []byte
	Seq       int64
	TS        int64
	Version   int64 // 1 when zero
	Ephemeral *bool // the kind's flag when nil
	ID        []byte
	CT        []byte // filler when nil
	Signer    *Device
}

// Envelope is the encoding of a signed envelope.
func Envelope(e Env) []byte {
	rule := link.Kinds[e.Kind]
	eph := rule.Ephemeral
	if e.Ephemeral != nil {
		eph = *e.Ephemeral
	}
	if e.Version == 0 {
		e.Version = 1
	}
	if e.ID == nil {
		e.ID = Filler(16)
	}
	if e.CT == nil {
		e.CT = Filler(48)
	}
	signer := e.Signer
	if signer == nil {
		signer = e.From
	}
	var to [][]byte
	for _, id := range e.To {
		to = append(to, canon.Str(id))
	}
	header := canon.Enc(canon.Int(uint64(e.Version)), e.Team, canon.Int(uint64(e.Epoch)), canon.Int(uint64(e.HeadSeq)), e.HeadHash, canon.Str(e.From.ID),
		canon.Int(uint64(e.Seq)), e.ID, canon.Int(uint64(e.TS)), canon.Str(e.Kind), canon.List(to...), canon.Bool(eph))
	nonce := Filler(12)
	sig := ed25519.Sign(signer.Priv, link.SignedBytes(link.ContextEnvelope, e.Team, header, nonce, e.CT))
	return canon.Enc(header, nonce, e.CT, sig)
}

// SignRequest sets the three headers that authenticate a relay request. The path is the request's target as sent.
func SignRequest(req *http.Request, d *Device, team []byte, nowMS int64, body []byte) {
	msg := link.RequestBytes(team, req.Method, req.URL.RequestURI(), nowMS, body)
	req.Header.Set("X-Straw-Device", d.ID)
	req.Header.Set("X-Straw-Time", strconv.FormatInt(nowMS, 10))
	req.Header.Set("X-Straw-Sig", hex.EncodeToString(ed25519.Sign(d.Priv, msg)))
}
