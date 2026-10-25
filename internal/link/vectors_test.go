package link

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/wayne930242/straw-relay/internal/canon"
)

// The vectors are pi-straw-boss's tests/link/vectors.json (see testdata/README.md). Every test here replays a part of them that the relay
// can decide from plaintext; what needs a team key or a device's own keys (sealed keys, labels, ciphertexts, seq and ts guards per receiver)
// belongs to the clients.

type typed struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v"`
}

type vectors struct {
	Contexts map[string]string `json:"contexts"`
	Canon    []struct {
		Name   string  `json:"name"`
		Fields []typed `json:"fields"`
		Hex    string  `json:"hex"`
	} `json:"canon"`
	CanonInvalid []struct {
		Name  string  `json:"name"`
		Hex   string  `json:"hex"`
		Count *int    `json:"count"`
		Read  *string `json:"read"`
	} `json:"canonInvalid"`
	Base32  []struct{ Hex, Text string } `json:"base32"`
	Devices map[string]struct {
		SignSeed      string `json:"signSeed"`
		ID            string `json:"id"`
		SignPub       string `json:"signPub"`
		BoxPub        string `json:"boxPub"`
		KeyRecord     string `json:"keyRecord"`
		KeyRecordHash string `json:"keyRecordHash"`
	} `json:"devices"`
	Team    string `json:"team"`
	Request struct {
		Team   string `json:"team"`
		Method string `json:"method"`
		Path   string `json:"path"`
		Time   int64  `json:"time"`
		Body   string `json:"body"`
		Signed string `json:"signed"`
		Signer string `json:"signer"`
		Sig    string `json:"sig"`
	} `json:"request"`
	Roster struct {
		Log   []string `json:"log"`
		Heads []struct {
			Seq  int64  `json:"seq"`
			Hash string `json:"hash"`
		} `json:"heads"`
		Final struct {
			Epoch      int64 `json:"epoch"`
			EpochStart int64 `json:"epochStart"`
			Members    []struct {
				ID       string `json:"id"`
				Role     string `json:"role"`
				Attended bool   `json:"attended"`
			} `json:"members"`
			Banned []string `json:"banned"`
		} `json:"final"`
		Invalid []struct {
			Name    string   `json:"name"`
			Entries []string `json:"entries"`
			Self    *string  `json:"self"`
			Reason  string   `json:"reason"`
			Seq     int      `json:"seq"`
		} `json:"invalid"`
	} `json:"roster"`
	Envelopes []struct {
		Envelope string `json:"envelope"`
		Header   string `json:"header"`
		Kind     string `json:"kind"`
	} `json:"envelopes"`
	Receive []struct {
		Name     string `json:"name"`
		Envelope string `json:"envelope"`
		State    struct {
			Entries int   `json:"entries"`
			Now     int64 `json:"now"`
		} `json:"state"`
		Expect string `json:"expect"`
	} `json:"receive"`
	Join struct {
		InvalidIds []string `json:"invalidIds"`
	} `json:"join"`
	Kinds []struct {
		Kind string `json:"kind"`
		Rule struct {
			Ephemeral bool   `json:"ephemeral"`
			WholeTeam bool   `json:"wholeTeam"`
			Sender    string `json:"sender"`
		} `json:"rule"`
	} `json:"kinds"`
}

func loadVectors(t *testing.T) *vectors {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return &v
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func encodeTyped(t *testing.T, x typed) []byte {
	t.Helper()
	switch x.T {
	case "str":
		var s string
		must(t, json.Unmarshal(x.V, &s))
		return canon.Str(s)
	case "int":
		var n uint64
		must(t, json.Unmarshal(x.V, &n))
		return canon.Int(n)
	case "bool":
		var b bool
		must(t, json.Unmarshal(x.V, &b))
		return canon.Bool(b)
	case "bytes":
		var s string
		must(t, json.Unmarshal(x.V, &s))
		return unhex(t, s)
	case "list", "struct":
		var items []typed
		must(t, json.Unmarshal(x.V, &items))
		var enc [][]byte
		for _, it := range items {
			enc = append(enc, encodeTyped(t, it))
		}
		if x.T == "list" {
			return canon.List(enc...)
		}
		return canon.Enc(enc...)
	}
	t.Fatalf("unknown typed field %q", x.T)
	return nil
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCanonVectors(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.Canon {
		var fields [][]byte
		for _, f := range c.Fields {
			fields = append(fields, encodeTyped(t, f))
		}
		got := canon.Enc(fields...)
		if hex.EncodeToString(got) != c.Hex {
			t.Errorf("%s: encoded %x, want %s", c.Name, got, c.Hex)
		}
		back, err := canon.Dec(got, len(fields))
		if err != nil || len(back) != len(fields) {
			t.Errorf("%s: decode: %v", c.Name, err)
		}
	}
	for _, c := range v.CanonInvalid {
		count := -1
		if c.Count != nil {
			count = *c.Count
		}
		fields, err := canon.Dec(unhex(t, c.Hex), count)
		if err == nil && c.Read != nil {
			switch *c.Read {
			case "int":
				_, err = canon.ReadInt(fields[0])
			case "bool":
				_, err = canon.ReadBool(fields[0])
			}
		}
		if !errors.Is(err, canon.ErrMalformed) {
			t.Errorf("%s: want a malformed error, got %v", c.Name, err)
		}
	}
	// A JavaScript client cannot sign beyond 2^53 - 1.
	if _, err := canon.ReadInt(canon.Int(canon.MaxSafeInt + 1)); err == nil {
		t.Error("an integer beyond 2^53-1 must be rejected")
	}
}

func TestDeviceIDVectors(t *testing.T) {
	v := loadVectors(t)
	for _, b := range v.Base32 {
		if got := Base32(unhex(t, b.Hex)); got != b.Text {
			t.Errorf("base32(%s) = %s, want %s", b.Hex, got, b.Text)
		}
	}
	for name, d := range v.Devices {
		signPub := unhex(t, d.SignPub)
		if got := DeviceID(signPub); got != d.ID {
			t.Errorf("%s: id %s, want %s", name, got, d.ID)
		}
		if !IDOK(d.ID) {
			t.Errorf("%s: its own id is not ok", name)
		}
		priv := ed25519.NewKeyFromSeed(unhex(t, d.SignSeed))
		if !bytes.Equal(priv.Public().(ed25519.PublicKey), signPub) {
			t.Errorf("%s: the seed does not give the public key", name)
		}
		rec, err := DecodeKeyRecord(unhex(t, d.KeyRecord))
		if err != nil || rec.ID != d.ID || !bytes.Equal(rec.SignPub, signPub) || !bytes.Equal(rec.BoxPub, unhex(t, d.BoxPub)) {
			t.Errorf("%s: key record %v %v", name, rec, err)
		}
		sum := sha256.Sum256(unhex(t, d.KeyRecord))
		if hex.EncodeToString(sum[:]) != d.KeyRecordHash {
			t.Errorf("%s: key record hash", name)
		}
	}
	for _, id := range v.Join.InvalidIds {
		if IDOK(id) {
			t.Errorf("%q must not be an id", id)
		}
	}
	// A key record whose id is not the hash of its signing key is refused.
	d := v.Devices["alice"]
	forged := canon.Enc([]byte(v.Devices["bob"].ID), unhex(t, d.SignPub), unhex(t, d.BoxPub))
	if _, err := DecodeKeyRecord(forged); err == nil {
		t.Error("a key record with another device's id must be refused")
	}
}

func TestRequestSignatureVector(t *testing.T) {
	v := loadVectors(t)
	r := v.Request
	got := RequestBytes(unhex(t, r.Team), r.Method, r.Path, r.Time, unhex(t, r.Body))
	if hex.EncodeToString(got) != r.Signed {
		t.Fatalf("request bytes %x, want %s", got, r.Signed)
	}
	pub := unhex(t, v.Devices[r.Signer].SignPub)
	if !VerifySig(pub, got, unhex(t, r.Sig)) {
		t.Error("the vector's signature must verify")
	}
	other := RequestBytes(unhex(t, r.Team), r.Method, r.Path+"1", r.Time, unhex(t, r.Body))
	if VerifySig(pub, other, unhex(t, r.Sig)) {
		t.Error("a signature over another path must not verify")
	}
	if VerifySig(pub, got, unhex(t, r.Sig)[:63]) || VerifySig(pub[:31], got, unhex(t, r.Sig)) {
		t.Error("malformed keys and signatures verify as false, never panic")
	}
}

func rawLog(t *testing.T, v *vectors, n int) [][]byte {
	t.Helper()
	var out [][]byte
	for _, h := range v.Roster.Log[:n] {
		out = append(out, unhex(t, h))
	}
	return out
}

func TestRosterLogVector(t *testing.T) {
	v := loadVectors(t)
	team := unhex(t, v.Team)
	st, rej := Replay(team, rawLog(t, v, len(v.Roster.Log)))
	if rej != nil {
		t.Fatalf("the honest log is refused: %+v", rej)
	}
	for i, h := range v.Roster.Heads {
		if hex.EncodeToString(st.Hashes[i]) != h.Hash {
			t.Errorf("hash of entry %d", i)
		}
	}
	if st.Head.Seq != int64(len(v.Roster.Log)-1) || hex.EncodeToString(st.Head.Hash) != v.Roster.Heads[len(v.Roster.Heads)-1].Hash {
		t.Errorf("head %+v", st.Head)
	}
	f := v.Roster.Final
	if st.Epoch != f.Epoch || st.EpochStart != f.EpochStart || len(st.Members) != len(f.Members) || len(st.Banned) != len(f.Banned) {
		t.Fatalf("final state epoch %d start %d members %d banned %d", st.Epoch, st.EpochStart, len(st.Members), len(st.Banned))
	}
	for _, m := range f.Members {
		got, ok := st.Members[m.ID]
		if !ok || got.Role != m.Role || got.Attended != m.Attended {
			t.Errorf("member %s: %+v", m.ID, got)
		}
	}
	for _, id := range f.Banned {
		if !st.Banned[id] {
			t.Errorf("%s must be banned", id)
		}
	}
}

// Every invalid log the relay can judge is refused with the reason and position the reference gives. Logs whose reason starts with `fork-`
// are the ones only a device that holds keys can see, and are skipped.
func TestRosterInvalidVectors(t *testing.T) {
	v := loadVectors(t)
	team := unhex(t, v.Team)
	checked := 0
	for _, c := range v.Roster.Invalid {
		if c.Self != nil {
			continue
		}
		var raws [][]byte
		for _, e := range c.Entries {
			raws = append(raws, unhex(t, e))
		}
		_, rej := Replay(team, raws)
		if rej == nil {
			t.Errorf("%s: accepted, want %s at %d", c.Name, c.Reason, c.Seq)
			continue
		}
		if rej.Reason != c.Reason || rej.Seq != c.Seq {
			t.Errorf("%s: got %s at %d, want %s at %d", c.Name, rej.Reason, rej.Seq, c.Reason, c.Seq)
		}
		checked++
	}
	if checked < 40 {
		t.Errorf("only %d invalid logs replayed", checked)
	}
}

// What the relay can decide of an envelope from its plaintext. The receivers' other reasons depend on state the relay does not keep (their own
// copy, last seq or ts taken, team keys), so for those the relay must accept.
var relayBlind = map[string]bool{
	"ok": true, "not-addressed": true, "stale-seq": true, "stale-ephemeral": true, "no-key": true, "bad-ciphertext": true, "bad-plaintext": true,
}

func TestEnvelopeHeaderVectors(t *testing.T) {
	v := loadVectors(t)
	team := unhex(t, v.Team)
	checked := 0
	for _, c := range v.Receive {
		st, rej := Replay(team, rawLog(t, v, c.State.Entries))
		if rej != nil {
			t.Fatal(rej)
		}
		env, err := DecodeEnvelope(unhex(t, c.Envelope))
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		want := c.Expect
		if relayBlind[want] {
			want = ""
		}
		// A queued kind never has seq 0 (the first taken seq must be past 0): the relay refuses it for any addressee.
		if c.Name == "a seq of 0 for a queued kind" {
			want = "bad-seq"
		}
		if got := CheckEnvelope(team, st, env, c.State.Now); got != want {
			t.Errorf("%s: relay says %q, want %q", c.Name, got, want)
		}
		checked++
	}
	if checked < 45 {
		t.Errorf("only %d envelope cases replayed", checked)
	}
}

func TestEnvelopeShapeVectors(t *testing.T) {
	v := loadVectors(t)
	for _, e := range v.Envelopes {
		env, err := DecodeEnvelope(unhex(t, e.Envelope))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(env.HeaderRaw, unhex(t, e.Header)) || env.Header.Kind != e.Kind {
			t.Errorf("%s: header", e.Kind)
		}
	}
	for _, k := range v.Kinds {
		rule, ok := Kinds[k.Kind]
		if !ok || rule.Ephemeral != k.Rule.Ephemeral || rule.WholeTeam != k.Rule.WholeTeam || rule.Sender != k.Rule.Sender {
			t.Errorf("kind %s: %+v, want %+v", k.Kind, rule, k.Rule)
		}
	}
	if len(Kinds) != len(v.Kinds) {
		t.Errorf("%d kinds, vectors have %d", len(Kinds), len(v.Kinds))
	}
	// Truncated and padded envelopes are not envelopes.
	raw := unhex(t, v.Envelopes[0].Envelope)
	for _, bad := range [][]byte{nil, raw[:len(raw)-1], append(append([]byte{}, raw...), 0), raw[:10]} {
		if _, err := DecodeEnvelope(bad); err == nil {
			t.Error("a damaged envelope decoded")
		}
	}
}
