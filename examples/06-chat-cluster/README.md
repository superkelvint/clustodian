# Partitioned WebSocket chat

Rooms hash to Clustodian partitions. One participant owns each room as
`LEADER`; a second participant remains `STANDBY`. The client discovers the
leader from the observed routing snapshot and reconnects after the owner
dies.

Run the complete process-level demonstration with:

```bash
scripts/demo.sh
```

The integration test drives the WebSocket endpoint, kills the active room
owner, and asserts that the reconnecting client reaches the replacement.
Clustodian coordinates ownership; chat history is intentionally in-memory.
