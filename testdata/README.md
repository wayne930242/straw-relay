# Test data

`vectors.json` is a copy of `tests/link/vectors.json` from [pi-straw-boss](https://github.com/wayne930242/pi-straw-boss), taken at the
merge commit `1d417c87` on pi-straw-boss main ("Merge feat/operate-wire: the operate wire, live kind class, per-session keys, SSE live event (todo #121 task 1)"; the file last changed in `93b3b18d997dde4fc6a64c765187cbd0a54ac849`).

The tests in `internal/link` replay the parts the relay can decide from plaintext: canonical encoding, device ids, the request signature,
roster verification (every invalid log except the `fork-*` ones, which need a device's own keys), the envelope header checks (the `board`, `caps`, `tower-reply`, `unit-stop`, and `operate` kinds' cases included), the kinds' rules (the `live` flag too), and the structure of the `board`, `tower` (4 or 5 fields), `caps`, `tower-reply`, and `operate` (`operateCases.structure`) plaintext cases.
To update it, copy the file again and change the commit above.
