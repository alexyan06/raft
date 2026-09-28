# Raft

My implementation of [Raft](https://raft.github.io/raft.pdf) for MIT 6.824
(Distributed Systems), Lab 2.

`raft/raft.go` is mine. Everything else — the test harness (`raft/config.go`,
`raft/test_test.go`), the persistence stub (`raft/persister.go`), and the
`labrpc`/`labgob` packages that simulate an unreliable network — is course-provided
scaffolding, included here so the tests actually run.

## Status

| Part | What it covers | State |
| --- | --- | --- |
| 2A | Leader election and heartbeats | Done, tests pass |
| 2B | Log replication (`Start`, `AppendEntries` consistency check) | Not started |
| 2C | Persistence (`persist` / `readPersist`) | Not started |

## Running the tests

The code uses relative imports (`../labrpc`), so it builds in GOPATH mode rather
than as a module:

```sh
cd raft
GO111MODULE=off go test -run 2A -count=1
```

Raft tests are timing-sensitive and can pass or fail nondeterministically, so it's
worth running them several times:

```sh
GO111MODULE=off go test -run 2A -count=10
```

## Layout

```
raft/
  raft.go        # the implementation
  util.go        # DPrintf debug logging (set Debug = 1 to enable)
  persister.go   # provided: saves/restores persisted state
  config.go      # provided: test harness, builds the simulated cluster
  test_test.go   # provided: the lab tests
labrpc/          # provided: simulated RPC over an unreliable network
labgob/          # provided: gob wrapper that warns about un-capitalized fields
```
