package server_test

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/wayne930242/straw-relay/internal/canon"
	"github.com/wayne930242/straw-relay/internal/linktest"
)

func TestTeamCreationNeedsAOneTimeToken(t *testing.T) {
	h := newHarness(t)
	founder := linktest.NewDevice(1)
	tm := linktest.NewTeam()
	entry := tm.Create(founder)
	body := func(tok string) map[string]any {
		return map[string]any{"team": tm.Hex(), "token": tok, "entry": b64(entry)}
	}

	if status, out := h.call(founder, tm.ID, "POST", "/v1/teams", body("")); status != 401 || out["error"] != "bad-token" {
		t.Errorf("no token: %d %v", status, out)
	}
	if status, out := h.call(founder, tm.ID, "POST", "/v1/teams", body("srt1_not-a-token")); status != 401 || out["error"] != "bad-token" {
		t.Errorf("wrong token: %d %v", status, out)
	}
	tok, err := h.st.CreateToken(h.now.Load())
	if err != nil {
		t.Fatal(err)
	}
	// An invalid create entry (an unattended founder) does not burn the token.
	bad := linktest.Entry(tm.ID, founder, linktest.EntryParams{Seq: 0, Prev: make([]byte, 32), Op: "create", Subject: &linktest.Subj{Dev: founder, Role: "admin", Attended: false}, Epoch: 1, Payload: linktest.Filler(60)})
	if status, out := h.call(founder, tm.ID, "POST", "/v1/teams", map[string]any{"team": tm.Hex(), "token": tok, "entry": b64(bad)}); status != 422 || out["error"] != "bad-founder" {
		t.Errorf("bad create: %d %v", status, out)
	}
	// A request signed by another device than the founder is refused.
	if status, out := h.call(linktest.NewDevice(9), tm.ID, "POST", "/v1/teams", body(tok)); status != 401 {
		t.Errorf("wrong signer: %d %v", status, out)
	}
	status, out := h.call(founder, tm.ID, "POST", "/v1/teams", body(tok))
	if status != 201 {
		t.Fatalf("create: %d %v", status, out)
	}
	if head := out["head"].(map[string]any); head["seq"] != float64(0) || head["hash"] == "" {
		t.Errorf("head: %v", out)
	}
	// The token is spent: a second team with it is refused.
	other := linktest.NewTeam()
	if status, out := h.call(founder, other.ID, "POST", "/v1/teams", map[string]any{"team": other.Hex(), "token": tok, "entry": b64(other.Create(founder))}); status != 401 || out["error"] != "bad-token" {
		t.Errorf("reused token: %d %v", status, out)
	}
	// The same team again with a fresh token is a conflict, and that token survives it.
	tok2, _ := h.st.CreateToken(h.now.Load())
	if status, out := h.call(founder, tm.ID, "POST", "/v1/teams", body(tok2)); status != 409 || out["error"] != "team-exists" {
		t.Errorf("same team: %d %v", status, out)
	}
	fresh := linktest.NewTeam()
	if status, out := h.call(founder, fresh.ID, "POST", "/v1/teams", map[string]any{"team": fresh.Hex(), "token": tok2, "entry": b64(fresh.Create(founder))}); status != 201 {
		t.Errorf("token after a conflict: %d %v", status, out)
	}
	// A malformed team id.
	if status, _ := h.call(founder, tm.ID, "POST", "/v1/teams", map[string]any{"team": "zz", "token": tok2, "entry": b64(entry)}); status != 400 {
		t.Errorf("bad team id: %d", status)
	}
}

func TestRequestSignatureIsChecked(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	path := "/v1/teams/" + x.Hex() + "/roster"
	get := func(dev *linktest.Device, tweak func(*http.Request)) int {
		status, _ := h.callRaw(dev, x.ID, "GET", path, nil, tweak)
		return status
	}
	if s := get(x.bob, nil); s != 200 {
		t.Fatalf("a member's signed request: %d", s)
	}
	if s := get(nil, nil); s != 401 {
		t.Errorf("no headers: %d", s)
	}
	if s := get(linktest.NewDevice(9), nil); s != 401 {
		t.Errorf("a stranger: %d", s)
	}
	// A member's id with another device's signature.
	if s := get(x.bob, func(r *http.Request) { r.Header.Set("X-Straw-Device", x.alice.ID) }); s != 401 {
		t.Errorf("another device's id: %d", s)
	}
	// The signature covers the query string, the path, the method, and the body.
	status, _ := h.callRaw(x.bob, x.ID, "GET", path+"?after=1", nil, func(r *http.Request) {
		r2, _ := http.NewRequest("GET", h.http.URL+path, nil)
		linktest.SignRequest(r2, x.bob, x.ID, h.now.Load(), nil)
		r.Header.Set("X-Straw-Sig", r2.Header.Get("X-Straw-Sig"))
	})
	if status != 401 {
		t.Errorf("a signature for another query: %d", status)
	}
	// Time: five minutes either way is the window.
	for _, c := range []struct {
		skew time.Duration
		want int
	}{{4 * time.Minute, 200}, {-4 * time.Minute, 200}, {6 * time.Minute, 401}, {-6 * time.Minute, 401}} {
		status, out := h.callRaw(nil, nil, "GET", path, nil, func(r *http.Request) {
			linktest.SignRequest(r, x.bob, x.ID, h.now.Load()+c.skew.Milliseconds(), nil)
		})
		if status != c.want {
			t.Errorf("skew %v: %d %v", c.skew, status, out)
		}
	}
	// A body signed for another body.
	raw := []byte(`{"envelope":"AAAA"}`)
	status, _ = h.callRaw(x.bob, x.ID, "POST", path[:len(path)-6]+"acks", raw, func(r *http.Request) {
		linktest.SignRequest(r, x.bob, x.ID, h.now.Load(), []byte(`{"envelope":"BBBB"}`))
	})
	if status != 401 {
		t.Errorf("a body that is not the signed one: %d", status)
	}
	// Another team's id in a path the signature does not name.
	other := linktest.NewTeam()
	status, _ = h.callRaw(x.bob, other.ID, "GET", "/v1/teams/"+x.Hex()+"/roster", nil, nil)
	if status != 401 {
		t.Errorf("a signature for another team: %d", status)
	}
	// An unknown team looks like a non-member.
	status, _ = h.callRaw(x.bob, other.ID, "GET", "/v1/teams/"+other.Hex()+"/roster", nil, nil)
	if status != 401 {
		t.Errorf("an unknown team: %d", status)
	}
	// A removed member is refused.
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	if s := get(x.bob, nil); s != 401 {
		t.Errorf("a removed member: %d", s)
	}
}

func TestRosterReadAndCompareAndSet(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	path := "/v1/teams/" + x.Hex() + "/roster"

	status, out := h.call(x.bob, x.ID, "GET", path, nil)
	entries := out["entries"].([]any)
	if status != 200 || len(entries) != 3 || out["head"].(map[string]any)["seq"] != float64(2) {
		t.Fatalf("roster: %d %v", status, out)
	}
	if e := entries[1].(map[string]any); e["seq"] != float64(1) || e["hash"] == "" || e["entry"] == "" {
		t.Errorf("entry shape: %v", e)
	}
	_, out = h.call(x.bob, x.ID, "GET", path+"?after=1", nil)
	if after := out["entries"].([]any); len(after) != 1 || after[0].(map[string]any)["seq"] != float64(2) {
		t.Errorf("after=1: %v", out)
	}
	_, out = h.call(x.bob, x.ID, "GET", path+"?after=2", nil)
	if after, _ := out["entries"].([]any); len(after) != 0 {
		t.Errorf("after=2: %v", out)
	}
	if status, _ := h.call(x.bob, x.ID, "GET", path+"?after=x", nil); status != 400 {
		t.Errorf("after=x: %d", status)
	}

	// Two writers race on the head: the second is a conflict that answers with the head.
	carol, eve := linktest.NewDevice(3), linktest.NewDevice(5)
	headBefore := append([]byte{}, x.Hash...)
	seqBefore := x.Seq
	first := x.Add(x.alice, carol, true)
	h.mustPost(x.alice, x.Team, first)
	x.Seq, x.Hash = seqBefore, headBefore // eve's entry was built on the old head
	second := x.Add(x.bob, eve, true)
	status, out = h.post(x.bob, x.Team, second)
	if status != 409 || out["error"] != "conflict" {
		t.Fatalf("conflict: %d %v", status, out)
	}
	head := out["head"].(map[string]any)
	if head["seq"] != float64(3) || head["hash"] != hex.EncodeToString(sha(first)) {
		t.Errorf("the conflict's head: %v", head)
	}
	// Posting the very same entry again is a conflict too, whose head shows it is the head.
	if status, out := h.post(x.alice, x.Team, first); status != 409 || out["head"].(map[string]any)["hash"] != hex.EncodeToString(sha(first)) {
		t.Errorf("a repeated entry: %d %v", status, out)
	}
	// The entry was signed by its sender: a request signed by another member for it is refused.
	x.Seq, x.Hash = 3, sha(first)
	third := x.Add(x.alice, eve, true)
	if status, out := h.post(x.bob, x.Team, third); status != 403 || out["error"] != "not-signer" {
		t.Errorf("a member posting another member's entry: %d %v", status, out)
	}
	h.mustPost(x.alice, x.Team, third)
	if status, out := h.call(x.bob, x.ID, "GET", path, nil); status != 200 || len(out["entries"].([]any)) != 5 {
		t.Errorf("after the appends: %v", out)
	}
	// Not an entry at all.
	if status, out := h.call(x.alice, x.ID, "POST", path, map[string]any{"entry": b64([]byte("junk"))}); status != 422 || out["error"] != "malformed" {
		t.Errorf("junk: %d %v", status, out)
	}
	if status, _ := h.call(x.alice, x.ID, "POST", path, map[string]any{"entry": "!!"}); status != 400 {
		t.Errorf("bad base64: %d", status)
	}
}

func TestRosterRulesAreEnforced(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	carol, mallory := linktest.NewDevice(3), linktest.NewDevice(6)
	reject := func(name string, signer *linktest.Device, entry []byte, reason string) {
		t.Helper()
		status, out := h.post(signer, x.Team, entry)
		if status != 422 || out["error"] != reason {
			t.Errorf("%s: %d %v, want 422 %s", name, status, out, reason)
		}
	}
	next := func(p linktest.EntryParams) linktest.EntryParams {
		p.Seq, p.Prev = x.Seq+1, x.Hash
		if p.Epoch == 0 {
			p.Epoch = x.Epoch
		}
		return p
	}
	sub := func(d *linktest.Device, role string, att bool) *linktest.Subj {
		return &linktest.Subj{Dev: d, Role: role, Attended: att}
	}

	reject("an unattended device signs nothing", x.dave, linktest.Entry(x.ID, x.dave, next(linktest.EntryParams{Op: "add", Subject: sub(carol, "member", true), Payload: linktest.AddPayload()})), "signer-unattended")
	reject("a member is not an admin", x.bob, linktest.Entry(x.ID, x.bob, next(linktest.EntryParams{Op: "rotate", Epoch: 2, Payload: linktest.RekeyPayload(x.alice.ID, x.bob.ID, x.dave.ID)})), "not-admin")
	if status, _ := h.post(mallory, x.Team, linktest.Entry(x.ID, mallory, next(linktest.EntryParams{Op: "add", Subject: sub(carol, "member", true), Payload: linktest.AddPayload()}))); status != 401 {
		t.Errorf("a stranger cannot even post: %d", status)
	}
	// A member posting an entry that names a stranger as signer is refused for who signed, before the rules.
	if status, out := h.post(x.alice, x.Team, linktest.Entry(x.ID, mallory, next(linktest.EntryParams{Op: "add", Subject: sub(carol, "member", true), Payload: linktest.AddPayload()}))); status != 403 || out["error"] != "not-signer" {
		t.Errorf("a stranger's entry: %d %v", status, out)
	}
	reject("an add gives the role member", x.alice, linktest.Entry(x.ID, x.alice, next(linktest.EntryParams{Op: "add", Subject: sub(carol, "admin", true), Payload: linktest.AddPayload()})), "bad-role")
	reject("an unattended device never becomes admin", x.alice, linktest.Entry(x.ID, x.alice, next(linktest.EntryParams{Op: "role", Subject: sub(x.dave, "admin", false), Payload: canon.Enc(canon.Str("admin"))})), "unattended-admin")
	reject("the last attended admin stays", x.alice, linktest.Entry(x.ID, x.alice, next(linktest.EntryParams{Op: "remove", Epoch: 2, Subject: sub(x.alice, "admin", true), Payload: linktest.RekeyPayload(x.bob.ID, x.dave.ID)})), "no-attended-admin")
	reject("a remove is the next epoch", x.alice, linktest.Entry(x.ID, x.alice, next(linktest.EntryParams{Op: "remove", Epoch: 1, Subject: sub(x.bob, "member", true), Payload: linktest.RekeyPayload(x.alice.ID, x.dave.ID)})), "bad-epoch")
	reject("sealed to exactly the remaining members", x.alice, linktest.Entry(x.ID, x.alice, next(linktest.EntryParams{Op: "remove", Epoch: 2, Subject: sub(x.bob, "member", true), Payload: linktest.RekeyPayload(x.alice.ID)})), "sealed-set-mismatch")
	reject("sealed to a stranger too", x.alice, linktest.Entry(x.ID, x.alice, next(linktest.EntryParams{Op: "rotate", Epoch: 2, Payload: linktest.RekeyPayload(x.alice.ID, x.bob.ID, x.dave.ID, carol.ID)})), "sealed-set-mismatch")
	reject("an add of a member again", x.alice, linktest.Entry(x.ID, x.alice, next(linktest.EntryParams{Op: "add", Subject: sub(x.bob, "member", true), Payload: linktest.AddPayload()})), "duplicate-member")

	// A removed id is banned for good.
	h.mustPost(x.alice, x.Team, x.Remove(x.alice, x.bob))
	reject("a removed id is banned", x.alice, linktest.Entry(x.ID, x.alice, next(linktest.EntryParams{Op: "add", Subject: sub(x.bob, "member", true), Payload: linktest.AddPayload()})), "banned")
	// After the rejections the log is intact and still takes a valid entry.
	h.mustPost(x.alice, x.Team, x.Add(x.alice, carol, true))
	status, out := h.call(x.alice, x.ID, "GET", "/v1/teams/"+x.Hex()+"/roster", nil)
	if status != 200 || len(out["entries"].([]any)) != 5 {
		t.Errorf("roster: %d %v", status, out)
	}
}

// The vectors' own log goes through the endpoint entry by entry, signed by whoever signed it.
func TestVectorRosterThroughTheEndpoint(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Team    string `json:"team"`
		Devices map[string]struct {
			SignSeed string `json:"signSeed"`
			ID       string `json:"id"`
		} `json:"devices"`
		Roster struct {
			Log   []string `json:"log"`
			Heads []struct{ Hash string }
		} `json:"roster"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	devs := map[string]*linktest.Device{}
	for _, d := range v.Devices {
		seed, _ := hex.DecodeString(d.SignSeed)
		dev := linktest.FromSeed(seed)
		devs[dev.ID] = dev
	}
	team, _ := hex.DecodeString(v.Team)
	h := newHarness(t)
	tok, _ := h.st.CreateToken(h.now.Load())
	for i, entryHex := range v.Roster.Log {
		entry, _ := hex.DecodeString(entryHex)
		fields, _ := canon.Dec(entry, 8)
		signer := devs[string(fields[4])]
		var status int
		var out map[string]any
		if i == 0 {
			status, out = h.call(signer, team, "POST", "/v1/teams", map[string]any{"team": v.Team, "token": tok, "entry": b64(entry)})
		} else {
			status, out = h.call(signer, team, "POST", "/v1/teams/"+v.Team+"/roster", map[string]any{"entry": b64(entry)})
		}
		if status != 201 {
			t.Fatalf("entry %d: %d %v", i, status, out)
		}
		if out["hash"] != nil && out["hash"] != v.Roster.Heads[i].Hash {
			t.Errorf("entry %d: hash %v", i, out["hash"])
		}
	}
}
