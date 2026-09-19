# Antithesis interface

The local runner is the reference workload and property implementation for
containerized deterministic exploration. An Antithesis deployment should start
the same clustodian-chaos-node controller, participant, and observer processes
against the etcd services in the deployment, then invoke:

    clustodian-chaos run --seed $ANTITHESIS_SEED --steps $ANTITHESIS_STEPS --profile nightly

The node processes are intentionally ordinary OS processes. The observer uses
its own etcd endpoint list, so the deployment can give it an unfaulted network
path while faulting controller and participant paths independently.

The local trace format is the hand-off boundary: it records the initial
configuration, ordered logical actions, selected targets, and fault parameters.
Antithesis may provide stronger scheduler and network replay inside its own
environment, but it is not required by the local PR or nightly profiles.
