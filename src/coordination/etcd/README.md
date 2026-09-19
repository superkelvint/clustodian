# etcd backend

This directory implements the etcd-backed coordination contract used by the
controller and participant runtimes.

## Key layout

All keys live below the configured namespace prefix. The important relative
paths are:

```text
metadata/<encoded key>                         persistent metadata
live/<encoded instance>                        lease-backed liveness
sessions/<encoded instance>/<session>          known session marker
current-state/<instance>/<session>/<resource>/<partition>
                                               session-owned replica state
internal/session-sequence                      logical session allocator
```

The transition queue is metadata at
`controller/output/pending-transitions`. The controller also publishes the
external view and its processed input revision as metadata.

Values in the metadata namespace are UTF-8 strings. Arbitrary identifier text
is encoded before it becomes part of a key, so `/` and other characters do not
change the namespace structure.

## Files to read

- `mod.rs` exposes `EtcdCoordination` and coordinates snapshots, metadata CAS,
  transition queue operations, and controller publication.
- `metadata.rs` contains ordinary metadata reads/writes and revision-guarded
  updates.
- `session.rs` creates lease-backed participant sessions, allocates logical
  session IDs, publishes session-owned state, and applies session fencing.
- `watch.rs` converts etcd events into semantic `WatchEvent` values and owns
  reconnect and compaction recovery.

Read `metadata.rs` first, then `session.rs`, then `watch.rs`. Return to the
larger façade in `mod.rs` after the smaller operations make sense.

## Recovery and fencing

A disconnected watch resumes from its last processed revision. A compacted
watch cannot resume from history, so the code takes a fresh authoritative
snapshot and starts a new watch after that snapshot.

Participant state publication compares the live session identity. An old
participant callback may finish later, but it cannot publish authoritative
state after its session has been replaced. Queue completion also requires the
queue key's modification revision to equal the captured queue revision, so a
callback cannot complete against a newer queue image after an intervening
write.

The CAS retry loops are bounded. `Contention` is an explicit failure rather
than an invitation to retry forever.
