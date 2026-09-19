# Job worker ownership

This example coordinates three work queues with Clustodian. Every queue has
one `LEADER` worker actively processing jobs and one `STANDBY` worker ready to
take over. The worker data plane is deliberately an in-memory counter.

Run the interactive demo with:

```bash
scripts/demo.sh
```

The script starts etcd, configures the queues, waits for convergence, submits
a job, kills the active owner, and shows the promoted worker. The integration
test performs the same flow against real controller and participant
processes.

Clustodian coordinates ownership; it does not provide a durable queue or
exactly-once job execution.
