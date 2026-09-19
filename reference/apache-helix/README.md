# Apache Helix Conformance Laboratory

This directory is the executable reference boundary for selected
cluster-management semantics. The external upstream source is Apache Helix tag
`helix-2.0.1` under `reference/apache-helix-2.0.1/`. That ignored tree is
read-only for this laboratory and must never be patched or attached to our
Maven reactor. Set `CLUSTODIAN_APACHE_HELIX_SOURCE` when the checkout lives
elsewhere.

## Build and isolation

`scripts/build-apache-helix-reference.sh` builds `helix-core` and its required modules
from the external source into `reference/apache-helix/.m2/`. The oracle Maven module is
standalone under `oracle/` and resolves `org.apache.helix:helix-core:2.0.1`
from that repository. It does not trust `~/.m2` and does not fetch another
Helix release.

`scripts/build-java-oracle.sh` compiles the oracle and records its runtime
classpath in the ignored Maven target directory. The normal production
`helix-core` artifact is sufficient; M0 does not consume Helix's test JAR.
Mockito is used only for accessor and manager plumbing, following the style of
the upstream stage tests.

The oracle inputs are versioned with the repository. The local Maven marker
records the Clustodian Git revision that produced the cached `helix-core`
artifact, and the build still checks that the installed artifact is
byte-identical to the external source build. The source checkout itself is
not committed; provision the exact Apache Helix `helix-2.0.1` tag before
running the Java verifiers.

## Oracle behavior

The Java command invokes these production classes directly:

- `BuiltInStateModelDefinitions.LeaderStandby` and `StateModelDefinition`;
- `MessageGenerationPhase`;
- `ClusterEvent`, `CurrentStateOutput`, `BestPossibleStateOutput`, and
  `MessageOutput`;
- `Resource`, `Partition`, `LiveInstance`, and `Message`.

The invocation pattern follows the focused upstream tests
`TestCancellationMessageGeneration`, `TestManagementMessageGeneration`,
`TestPrioritizationMessageGeneration`, and `TestStateModelValidity`. The
controller decision is always made by Helix production code; mocks provide only
environmental data such as live-instance sessions and manager accessors.

M0 is ZooKeeper-free. It does not prove controller convergence, placement,
rebalancing, failure handling, or participant execution.

## Protocol

Scenario and result documents are version 1 and are described by:

- `schema/scenario-v1.schema.json`;
- `schema/result-v1.schema.json`.

`run-java-oracle.sh <scenario.json>` prints only compact canonical result JSON
to stdout and sends diagnostics to stderr. Transition results expose only
resource, partition, target instance, from-state, to-state, and semantic
message type. Results are explicitly sorted by semantic fields; Helix message
IDs and timestamps are excluded.

## Fixtures and verification

M0 scenarios live in `scenarios/m0/`, with generated expected results in
`expected/m0/`. Run `scripts/regenerate-m0-expected.sh` only when intentionally
updating the fixture corpus. Normal verification never regenerates expected
files:

```text
./reference/apache-helix/verify-m0.sh
```

The verifier rebuilds the pinned artifact and oracle, executes every scenario,
compares canonical JSON, and fails on any mismatch. The acceptance evidence
does not regenerate expected files or silently skip a scenario. If the
expected corpus changes, review that change as a fixture update and rerun the
verifier from a clean checkout.

## Testing ladder

```text
M0      in-process state model + MessageGenerationPhase oracle; no ZooKeeper
M1      Rust generic StateModelDefinition
M2      Rust semantic next-step transition generation
M3      Rust SEMI_AUTO explicit-placement BestPossibleState
M4      explicit assignment models and automatic placement
later   full convergence oracle with controller, participants, and ZooKeeper
```

M0 proves only that selected Apache Helix 2.0.1 state-model and
transition-generation behavior can be executed and captured as deterministic,
language-neutral results for future Rust differential testing.

## M1 differential gate

M1 adds the pure Rust state-model kernel in `src/` and the
`inspect_state_model` path in `clustodian-conformance`. The exhaustive scenario
under `scenarios/m1/` compares all 16 ordered LeaderStandby state pairs against
the Java oracle. Run:

```text
./reference/apache-helix/verify-m1.sh
```

M1 does not add Rust transition generation, placement, rebalancing,
participants, persistence compatibility, or controller stages.

## M2 differential gate

M2 adds the pure Rust `CurrentState`, `BestPossibleState`, typed cluster
identities, `TransitionRequest`, and semantic next-step transition generation.
The target state is input; M2 does not calculate assignment or placement.
Scenarios under `scenarios/m2/` compare the Rust result directly with the real
Apache Helix 2.0.1 `MessageGenerationPhase` oracle. The verifier canonicalizes
transition ordering and compares only resource, partition, instance, from,
to, and message type:

```text
./reference/apache-helix/verify-m2.sh
```

The M2 source boundary studied for this subset is:

- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/stages/MessageGenerationPhase.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/stages/MessageOutput.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/stages/CurrentStateOutput.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/stages/BestPossibleStateOutput.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/dataproviders/BaseControllerDataProvider.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/dataproviders/ResourceControllerDataProvider.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/stages/ClusterEvent.java`.

The focused upstream tests used as behavioral references are
`TestCancellationMessageGeneration`, `TestManagementMessageGeneration`,
`TestP2PStateTransitionMessages`, and `TestRedundantDroppedMessage` under the
pinned `helix-core/src/test/java/org/apache/helix/` tree.

The LeaderStandby `DROPPED -> LEADER` case is checked in as the unreachable M2
scenario. Helix 2.0.1 emits an “unable to find a next state” diagnostic while
processing it; the oracle captures that output and redirects it to stderr so
stdout remains canonical JSON. Both implementations therefore prove that a
missing state-model path produces no generated transition.

M2 does not prove target-state calculation, automatic placement, transition
concurrency safety, transition selection or throttling, controller
convergence, participant execution, failure detection, message transport,
sessions, or ZooKeeper behavior.

## M3 differential gate

M3 adds the pure Rust `IdealState` explicit preference-list representation and
deterministic SEMI_AUTO best-possible-state calculation. The target placement
and live instances are inputs; M3 does not calculate automatic placement or
rebalance. `compute_semi_auto_best_possible` invokes Helix 2.0.1 production
`SemiAutoRebalancer.computeBestPossiblePartitionState` in the Java oracle and
compares the semantic `best_possible_state` map against Rust:

```text
./reference/apache-helix/verify-m3.sh
```

The Rust subset ports the shared assignment logic in
`reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/rebalancer/AbstractRebalancer.java`,
with the SEMI_AUTO entry point in
`reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/rebalancer/SemiAutoRebalancer.java`.
The oracle construction uses `IdealState.java`, `ResourceAssignment.java`,
`CurrentStateOutput.java`, and `BaseControllerDataProvider.java` from the same
pinned tree. Behavioral test references are
`helix-core/src/test/java/org/apache/helix/integration/rebalancer/TestSemiAutoRebalance.java`,
`helix-core/src/test/java/org/apache/helix/controller/stages/TestBestPossibleStateCalcStage.java`,
and
`helix-core/src/test/java/org/apache/helix/controller/rebalancer/TestPreferenceListNodeComparatorWithTopologyAware.java`.

M3 does not prove automatic placement, movement minimization, topology or
capacity-aware assignment, transition safety or throttling, controller
convergence, participant execution, failure detection, message transport,
ExternalView, Java API compatibility, or ZNRecord/persistence compatibility.

## M4 differential gate

M4 adds the pure Rust CRUSH placement kernel. It consumes the resource's
partitions, state-count-derived replica factor, topology-aware instance
domains, and live-instance set, and returns Helix-ordered preference lists.
It does not assign replica states or generate transitions. Run:

```text
./reference/apache-helix/verify-m4.sh
```

The Java oracle invokes the real
`org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy`.
The Rust port is based on these pinned Helix 2.0.1 files:

- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/rebalancer/strategy/CrushRebalanceStrategy.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/controller/rebalancer/strategy/crushMapping/CRUSHPlacementAlgorithm.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/topology/Topology.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/topology/Node.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/topology/InstanceNode.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/util/JenkinsHash.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/model/ClusterTopologyConfig.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/model/ClusterConfig.java`;
- `reference/apache-helix-2.0.1/helix-core/src/main/java/org/apache/helix/model/InstanceConfig.java`.

Behavioral references include
`helix-core/src/test/java/org/apache/helix/integration/rebalancer/CrushRebalancers/TestCrushAutoRebalance.java`,
`TestCrushAutoRebalanceNonRack.java`,
`TestCrushAutoRebalanceTopoplogyAwareDisabled.java`,
`TestNodeSwap.java`,
`helix-core/src/test/java/org/apache/helix/controller/rebalancer/TestPreferenceListNodeComparatorWithTopologyAware.java`,
and `helix-core/src/test/java/org/apache/helix/controller/strategy/TestTopology.java`.

M4 matches the supplied topology-aware CRUSH domain (`/zone/instance`) and
does not claim FULL_AUTO, WAGED, capacity-aware placement, delayed rebalance,
state assignment, transition generation, controller convergence, or runtime
coordination. Preference-list order is semantic; insufficient live fault
zones can therefore produce fewer than the requested replica factor, as in
the upstream strategy.
