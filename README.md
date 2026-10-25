# straw-relay

The relay for Straw Link, the cross-computer tower link of [pi-straw-boss](https://github.com/wayne930242/pi-straw-boss). It routes
end-to-end encrypted envelopes between a team's computers and never decrypts them: it checks Ed25519 signatures and roster rules, stores
ciphertext for a few days, and streams it to the computers it is addressed to. A single Go binary, SQLite through
[modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) (no cgo), and a Dockerfile.

TLS is the ingress's job: the relay speaks plain HTTP.

## Run

```sh
go build -o straw-relay ./cmd/straw-relay

./straw-relay token create --db relay.db     # a one-time team token, printed once; hand it to the person who creates a team
./straw-relay serve --addr :8080 --db relay.db
```

With Docker (the database lives in the `/data` volume; run `token create` in the running container):

```sh
docker build -t straw-relay .
docker run -d --name straw-relay -p 8080:8080 -v straw-relay-data:/data straw-relay
docker exec straw-relay /straw-relay token create
```

`STRAW_RELAY_ADDR` and `STRAW_RELAY_DB` set the defaults of `--addr` and `--db`. `GET /healthz` answers `{"ok":true}`.
Logs are JSON lines on stdout: method, route, team id, device id, status, request and response sizes, and the refusal reason. Never a body,
a token, or a query string.

Server limits: 10 s to send the headers, 30 s for a whole request, 30 s per response write (a stream extends its own deadline on every
write), 120 s idle on a kept-alive connection, 16 KB of headers.

## Endpoints

| Endpoint | What it does |
|---|---|
| `POST /v1/teams` | Create a team from its `create` entry and an operator token. |
| `GET /v1/teams/{team}/roster?after=seq` | Entries with seq greater than `after` (all when absent), and the head. |
| `POST /v1/teams/{team}/roster` | Append an entry by compare and set on `prev`. |
| `POST /v1/teams/{team}/envelopes` | Store an envelope for its `to` devices, or for the whole team. |
| `GET /v1/teams/{team}/events` | Server-sent events for the calling device. |
| `POST /v1/teams/{team}/acks` | Mark queued envelopes read by the calling device. |
| `POST /v1/invites` | Hold the hash of an invite secret until it expires. |
| `POST /v1/invites/{h}/join` | Take the invite's one join request and pass it to the inviter's stream. No signature. |
| `GET /healthz` | Liveness. |

Team ids and invite hashes in paths are lower-case hex; `h` is `sha256(invite secret)`.

### Wire format

JSON is only the wrapper; everything signed or hashed is the canonical byte string of the Straw Link spec. Fixed-size values (team ids, hashes,
envelope ids, signatures) are lower-case hex; variable-length byte strings (roster entries, envelopes, join requests) are base64url without
padding. Device ids are the 26-character base32 of the spec.

**Authentication.** Every request except a join carries

| Header | Value |
|---|---|
| `X-Straw-Device` | the caller's device id |
| `X-Straw-Time` | milliseconds since the epoch, decimal |
| `X-Straw-Sig` | hex Ed25519 signature over `"straw-link/request/1" ‖ team ‖ canonical(method, request target, time, sha256(body))` |

The request target is the path with its query string, exactly as sent (the signature is checked over the bytes received). The relay accepts it
only from a current, non-removed member of the team, within 5 minutes of its clock. `POST /v1/teams` and `POST /v1/invites` name the team in
the body, and `POST /v1/teams` is checked against the founder's key in the `create` entry. A request replayed inside the window is
harmless: envelopes dedupe by id, a roster append is compare and set, and the other writes are idempotent or refused as duplicates.

**Errors** are `{"error": "<code>"}` with these statuses: 400 malformed input, 401 authentication, 403 not allowed, 404 no such invite,
409 conflict, 413 body over the hard limit, 422 a roster entry or envelope the rules refuse (the code is the rule's reason), 429 a cap.
A 429 carries `Retry-After`.

| Endpoint | Request body | Success | Other answers |
|---|---|---|---|
| `POST /v1/teams` | `{"team": hex16, "token": "srt1_…", "entry": b64}` | 201 `{"team", "head": {"seq": 0, "hash"}}` | 401 `bad-token`; 409 `team-exists`; 422 the create entry's reason |
| `GET …/roster` | none | 200 `{"head": {"seq", "hash"}, "entries": [{"seq", "hash", "entry": b64}]}` | 400 `bad-after` |
| `POST …/roster` | `{"entry": b64}` | 201 `{"seq", "hash"}` | 403 `not-signer`; 409 `conflict` with `"head": {"seq", "hash"}`; 422 reason with `"seq"` |
| `POST …/envelopes` | `{"envelope": b64}` | 201 `{"id": hex16, "duplicate": false}`; 200 with `"duplicate": true` for an id already stored, or `"stale": true` for an ephemeral envelope no newer than the kept one | 400 `malformed`; 403 `from-mismatch`; 422 header reason or `no-addressee`; 429 `rate-limited`, `envelope-too-large`, `team-full` |
| `POST …/acks` | `{"ids": [hex16, …]}` (at most 500) | 200 `{"acked": n}` | 400 |
| `POST /v1/invites` | `{"team": hex16, "h": hex32, "expires": ms}` | 201 `{"expires"}` | 403 `not-attended`; 409 `invite-exists`; 422 `bad-expiry`; 429 `too-many-invites` |
| `POST /v1/invites/{h}/join` | `{"request": b64}` (at most 4096 bytes decoded) | 202 `{"queued": true}` | 404 `no-invite` (unknown or expired); 409 `invite-used`; 413 |

A roster entry is the `(seq, prev, op, subject, by, epoch, payload, sig)` encoding of the spec; the relay appends it only when `prev` is the head
and every rule it can check from plaintext holds. A conflict answers with the head so the client can fetch what it missed.

### Events

`GET /v1/teams/{team}/events` is `text/event-stream`. Every event has an `id:` that only grows (one counter per relay); `Last-Event-ID` resumes
after it. Without it the stream starts from the beginning: every roster entry, every queued envelope not yet acknowledged, the last ephemeral
envelope of each sender and kind, and any waiting join requests. A `: keepalive` comment line goes out every 25 seconds.

| Event | Data | Sent to |
|---|---|---|
| `roster` | `{"seq", "hash", "entry": b64}` | every member |
| `envelope` | `{"id": hex16, "from": deviceId, "ephemeral": bool, "envelope": b64}` | the addressees; for an ephemeral envelope addressed to the whole team, every member but the sender |
| `join` | `{"hash": hex32, "request": b64}` | the inviter |

Queued envelopes stay until the device acknowledges them (`POST …/acks`), whether or not it saw them on a stream, and are dropped after
7 days. A device removed from the team is shown the roster entries it has not seen, its own removal among them, and the stream ends.
A device may hold up to 4 streams; opening a fifth ends the oldest.

## What the relay checks

- **Requests**: the signature above, over the bytes received.
- **Roster entries**: signer is a current member with the authority for the op; an unattended device signs nothing; no `remove` or `role`
  leaves the team without an attended admin; a removed id is banned; `add` gives the role `member`; an unattended device never becomes admin;
  `remove` and `rotate` move to epoch + 1 and seal to exactly the remaining members; subject records match the roster. Entries are decoded
  and hashed as received.
- **Envelopes**: version, team, a known kind, the ephemeral flag the kind fixes, device ids in canonical form, the sender is a current member
  and signed it, the kind is one that sender's `attended` flag allows, the epoch (current, or the previous one when the sender's roster head
  predates the rotation), whole-team only for kinds that allow it, `seq` is 0 for ephemeral kinds and at least 1 for queued ones, the
  roster head exists, `ts` is at most 10 minutes ahead and at most 7 days back. The envelope's `from` has to be the caller.
- **Caps per team**: 50 MB stored, 256 KB per envelope, 600 envelopes a minute; over a cap the answer is 429. Open invites: 50 per team.

What a receiver decides from its own state stays with the receivers: whether an envelope is addressed to it, the `seq` and `ts` guards, the
team key and the ciphertext, sealed keys and label tables in roster entries.

## Development

```sh
go vet ./...
go test ./...
```

`testdata/vectors.json` is a copy of the test vectors of pi-straw-boss (see `testdata/README.md` for the source commit). The tests replay
them for the canonical encoding, device ids, the request signature, roster verification, and envelope headers, and exercise every
endpoint through `httptest`.

## License

MIT
