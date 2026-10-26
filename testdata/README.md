# Test data

`vectors.json` is a copy of `tests/link/vectors.json` from [pi-straw-boss](https://github.com/wayne930242/pi-straw-boss), taken at the
last commit that changed that file: `fff31b1e3f930523daebf2cff06f9335db78a575`
("feat(link): the board message kind and its vectors", on pi-straw-boss branch feat/straw-link-board).

The tests in `internal/link` replay the parts the relay can decide from plaintext: canonical encoding, device ids, the request signature,
roster verification (every invalid log except the `fork-*` ones, which need a device's own keys), the envelope header checks (the `board` kind's cases included), and the structure of the `board` plaintext cases.
To update it, copy the file again and change the commit above.
