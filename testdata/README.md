# Test data

`vectors.json` is a copy of `tests/link/vectors.json` from [pi-straw-boss](https://github.com/wayne930242/pi-straw-boss), taken at the
last commit that changed that file: `bdc569b5a1df8a245e958f946f28ea68ac88f9db`
("feat(link): caps and tower-reply kinds, optional awaitsReply field of tower (wire format only)", on pi-straw-boss branch feat/link-kinds-caps-reply).

The tests in `internal/link` replay the parts the relay can decide from plaintext: canonical encoding, device ids, the request signature,
roster verification (every invalid log except the `fork-*` ones, which need a device's own keys), the envelope header checks (the `board`, `caps`, and `tower-reply` kinds' cases included), and the structure of the `board`, `tower` (4 or 5 fields), `caps`, and `tower-reply` plaintext cases.
To update it, copy the file again and change the commit above.
