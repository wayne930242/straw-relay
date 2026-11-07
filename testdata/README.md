# Test data

`vectors.json` is a copy of `tests/link/vectors.json` from [pi-straw-boss](https://github.com/wayne930242/pi-straw-boss), taken at the
last commit that changed that file: `57d5fadb24177e7d7ccf11736b514c1b5273c890`
("fix(link): unit-stop review fixes: one-line reason covers C1, bidi, and invisible characters; caps list unit-stop only with a sink; a close uses the resolved session id; tests wait on signals", on pi-straw-boss main since 927c83ce).

The tests in `internal/link` replay the parts the relay can decide from plaintext: canonical encoding, device ids, the request signature,
roster verification (every invalid log except the `fork-*` ones, which need a device's own keys), the envelope header checks (the `board`, `caps`, `tower-reply`, and `unit-stop` kinds' cases included), and the structure of the `board`, `tower` (4 or 5 fields), `caps`, and `tower-reply` plaintext cases.
To update it, copy the file again and change the commit above.
