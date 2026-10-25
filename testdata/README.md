# Test data

`vectors.json` is a copy of `tests/link/vectors.json` from [pi-straw-boss](https://github.com/wayne930242/pi-straw-boss), taken at the
last commit that changed that file: `4022c90f4eb4678cd7ebe6cef5b9d9c3199e4d3b`
("fix(link): reject non-canonical base32 device ids; vectors for ids and the pinned device").

The tests in `internal/link` replay the parts the relay can decide from plaintext: canonical encoding, device ids, the request signature,
roster verification (every invalid log except the `fork-*` ones, which need a device's own keys), and the envelope header checks.
To update it, copy the file again and change the commit above.
