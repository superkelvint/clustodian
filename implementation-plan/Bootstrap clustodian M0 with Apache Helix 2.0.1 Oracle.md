You are starting a new standalone-quality Rust subproject inside the existing Clustodian repository.

The project is:

    src/

Apache Helix 2.0.1 source has already been extracted locally at:

    reference/apache-helix-2.0.1/

Treat that directory as READ ONLY.

This task is M0: establish the project boundary and build a trustworthy executable Apache Helix oracle BEFORE implementing substantive Rust behavior.

Do not redesign the plan.

The architectural investigation has already been done. Follow the decisions below.

# 1. Project purpose

`clustodian` is a standalone Rust implementation of the subset of Apache Helix cluster-management semantics that Clustodian actually needs.

There are two simultaneous requirements.

## Standalone architecture

`clustodian` must be generic.

It must be possible to extract:

    src/

into an unrelated repository tomorrow and use it to manage replicated resources for a database, filesystem, queue, cache, or other distributed application.

It MUST NOT depend on Clustodian.

It MUST NOT contain Clustodian-specific concepts such as:

    StateRootId
    PartitionHistoryId
    PublicationSequence
    QueryReadView
    MutationId
    SearchRequest
    index segments
    postings
    ANN
    schema migrations

Dependency direction must always be:

    clustodian
        ^
        |
    Clustodian cluster adapter
        ^
        |
    Clustodian

Never:

    clustodian -> Clustodian

## Relentlessly Search-Kernel-driven scope

Standalone architecture does NOT mean we are porting all of Apache Helix.

A Helix capability enters `clustodian` only because Clustodian has a concrete need for that generic cluster-management capability.

Do NOT implement features merely to achieve completeness with Apache Helix.

This principle must be prominent in:

    src/README.md

Use wording equivalent to:

    clustodian is standalone in architecture but demand-driven in scope.

    A Helix feature is implemented only when Clustodian has a concrete
    need for that class of generic cluster-management behavior.

    We do not implement Helix features merely for compatibility completeness.

# 2. What belongs in clustodian

Generic cluster-control capabilities such as:

- Participant / node identity
- Resource identity
- Partition identity
- replica assignment
- replica role/state models
- IdealState / desired state
- CurrentState / observed state
- ExternalView / routing state
- controller reconciliation
- legal state transitions
- transition generation
- transition ordering
- replica placement
- automatic rebalance
- node join/leave handling
- node failure handling
- draining
- relocation
- movement minimization
- failure-domain/topology-aware placement
- capacity-aware placement if Clustodian eventually requires it
- transition throttling if Clustodian eventually requires it
- convergence after interrupted cluster operations

# 3. What does NOT belong in clustodian

Clustodian remains responsible for its data plane and application semantics, including:

- mutation replication protocol
- write acknowledgement semantics
- WAL/log transport
- replica snapshot transfer
- replica bootstrap implementation
- mutation catch-up implementation
- authoritative storage
- durable/searchable publication
- StateRoot semantics
- publication sequence semantics
- PartitionHistoryId semantics
- Clustodian authority/fencing implementation
- replica data validation
- distributed search RPC
- query fanout
- result merging
- distributed aggregation/refinement
- index/schema migration
- physical index lifecycle

`clustodian` may eventually decide:

    partition P17 on participant B should transition FOLLOWER -> LEADER

Clustodian decides what executing that transition means.

# 4. Relationship to Apache Helix

Apache Helix 2.0.1 is:

1. the architectural reference;
2. the semantic reference;
3. the executable behavioral oracle for Helix-derived capabilities.

Do NOT claim global compatibility with Apache Helix.

Compatibility is feature-scoped.

For each implemented Helix-derived behavior, we should be able to say exactly which behavior is differential-tested against the real Apache Helix 2.0.1 implementation.

Use Helix terminology initially where it represents genuine architecture:

    Participant
    Spectator
    Controller
    Resource
    Partition
    StateModelDefinition
    IdealState
    CurrentState
    ExternalView
    Rebalancer

Do not gratuitously rename these while porting semantics.

Do NOT mechanically reproduce Java implementation structure, inheritance, beans, reflection, ZooKeeper paths, or Java-specific plumbing.

# 5. M0 concrete architecture decision

M0 must be ZOOKEEPER-FREE.

Do NOT use:

    ZkTestBase
    ClusterControllerManager
    MockParticipantManager
    ZkHelixClusterVerifier
    BestPossibleExternalViewVerifier

during M0.

Those belong to a later full integration/convergence oracle.

M0 instead exercises real Helix controller/model code directly in-process using explicitly prepared state.

The first meaningful controller oracle is:

    MessageGenerationPhase

Note carefully:

    MessageGenerationPhase

NOT:

    MessageGenerationStage

The real Helix 2.0.1 source is authoritative for exact package names and method signatures.

# 6. Why MessageGenerationPhase is the M0 oracle

The first substantive behavior we want to prove is:

    Given:
        StateModelDefinition
        CurrentState
        target/best-possible state

    What state transition messages does real Helix generate?

Conceptually:

    current state
          +
    target state
          +
    StateModelDefinition
          |
          v
    Apache Helix 2.0.1
    MessageGenerationPhase
          |
          v
    MessageOutput
          |
          v
    canonical transition list

This behavior is directly relevant to Clustodian because we need generic lifecycle transitions such as:

    OFFLINE -> FOLLOWER
    FOLLOWER -> LEADER
    LEADER -> FOLLOWER
    FOLLOWER -> DROPPED

without asking coding agents to invent transition behavior.

# 7. Helix classes relevant to M0

Inspect the exact Helix 2.0.1 source locally as needed to reproduce the correct invocation pattern, but do NOT reconsider the overall design.

The important production/model classes are expected to include the actual 2.0.1 equivalents of:

    StateModelDefinition
    Resource
    Partition
    CurrentStateOutput
    BestPossibleStateOutput
    IntermediateStateOutput
    MessageOutput
    ResourceControllerDataProvider
    ClusterEvent
    MessageGenerationPhase

Use the exact package/class names found in:

    reference/apache-helix-2.0.1/

Study the upstream Helix tests that directly exercise `MessageGenerationPhase` and closely follow their setup pattern.

Mocks are allowed for environment/accessor objects where Helix's own tests use mocks.

Mocks MUST NOT implement or substitute any cluster-management decision.

Acceptable mock:

    HelixManager
    HelixDataAccessor
    controller data accessor needed only to satisfy stage plumbing

Unacceptable mock:

    "when asked for desired transition, return FOLLOWER -> LEADER"

The decision must come from actual Helix code.

# 8. StateModelDefinition smoke oracle

In addition to the main `MessageGenerationPhase` oracle, provide a tiny StateModelDefinition oracle/smoke path.

It should expose actual Helix semantics such as, where supported by the real API:

- initial state;
- state priorities;
- legal transition graph;
- next state on the path from state A toward state B;
- state count/bound metadata where applicable.

This is useful for proving that a later Rust implementation has not invented its own state-machine traversal.

Do not reimplement graph traversal in the adapter.

Invoke real `StateModelDefinition` behavior.

# 9. Do NOT implement placement yet

M0 does NOT include:

    BestPossibleStateCalcStage
    automatic placement
    FULL_AUTO
    WAGED
    delayed rebalance
    topology placement
    capacity placement

These are later milestones.

M0 provides the measuring instrument needed before implementing the Rust model/controller.

# 10. Repository structure

Use this structure:

    clustodian/
    |
    |-- crates/
    |   `-- clustodian/
    |       |-- Cargo.toml
    |       |-- README.md
    |       `-- src/
    |           `-- lib.rs
    |
    |-- tools/
    |   `-- clustodian-conformance/
    |       |-- Cargo.toml
    |       `-- src/
    |           `-- main.rs
    |
    `-- reference/
        |
        |-- helix-2.0.1/
        |       # existing pristine upstream source
        |
        `-- helix/
            |-- README.md
            |
            |-- oracle/
            |   |-- pom.xml
            |   `-- src/
            |       `-- main/
            |           `-- java/
            |               `-- ...
            |
            |-- schema/
            |   |-- scenario-v1.schema.json
            |   `-- result-v1.schema.json
            |
            |-- scenarios/
            |   `-- m0/
            |
            |-- expected/
            |   `-- m0/
            |
            |-- scripts/
            |   |-- build-apache-helix-reference.sh
            |   |-- run-java-oracle.sh
            |   `-- regenerate-m0-expected.sh
            |
            `-- verify-m0.sh

Do not place the generic implementation under Clustodian's root `src/`.

Do not put Java-conformance concepts into production `clustodian`.

# 11. Future Rust module architecture

The intended eventual production structure is approximately:

    src/src/

        lib.rs
        error.rs

        model/
            mod.rs
            instance.rs
            resource.rs
            partition.rs
            state.rs
            state_model.rs
            ideal_state.rs
            current_state.rs
            external_view.rs

        controller/
            mod.rs
            event.rs
            cache.rs
            pipeline.rs
            stages/
                mod.rs
                resource_computation.rs
                current_state.rs
                best_possible_state.rs
                intermediate_state.rs
                message_generation.rs
                message_selection.rs
                message_throttle.rs
                external_view.rs

        rebalance/
            mod.rs

        transition/
            mod.rs
            message.rs
            constraints.rs
            throttle.rs

        routing/
            mod.rs
            routing_table.rs

        participant/
            mod.rs
            state_machine.rs

        gateway/
            mod.rs

        metadata/
            mod.rs

        membership/
            mod.rs

IMPORTANT:

THIS IS AN ARCHITECTURAL DESTINATION.

DO NOT create all of these empty files during M0.

During M0 create only:

    src/Cargo.toml
    src/README.md
    src/src/lib.rs

plus minimal workspace plumbing.

No substantive Helix Rust implementation is allowed yet.

# 12. Start with ONE Rust crate

Do not create:

    clustodian-model
    helix-controller
    helix-rebalancer
    helix-gateway
    helix-etcd
    helix-openraft

Use one production crate:

    src/

Module boundaries come first.

Crate extraction happens later only if real dependency/runtime boundaries justify it.

# 13. Rust conformance tool boundary

Create:

    tools/clustodian-conformance/

only as a thin future protocol boundary.

It will eventually:

    scenario JSON
        ->
    clustodian
        ->
    canonical result JSON

It must depend on `clustodian`.

`clustodian` must NEVER depend on the conformance tool.

M0 does not require this tool to implement actual Helix behavior because substantive Rust behavior does not exist yet.

A minimal executable/skeleton is sufficient if useful for establishing workspace structure.

Do not create fake behavior merely so it emits the same fixture as Java.

# 14. Java oracle build strategy

Do NOT alter:

    reference/apache-helix-2.0.1/**

Do NOT attach our source files to the upstream Maven reactor.

Do NOT copy Helix production or test classes.

Use a standalone Maven module:

    reference/apache-helix/oracle/

The oracle depends on the actual `helix-core` built from:

    reference/apache-helix-2.0.1/

# 15. Isolated Maven repository

Do not trust the developer's global Maven cache for the Helix artifact.

Build the pinned Helix source into a dedicated local Maven repository under:

    reference/apache-helix/.m2/

Create:

    reference/apache-helix/scripts/build-apache-helix-reference.sh

It should conceptually run:

    mvn \
      -Dmaven.repo.local=<repo>/reference/apache-helix/.m2 \
      -f <repo>/reference/apache-helix-2.0.1/pom.xml \
      -pl helix-core \
      -am \
      install \
      -DskipTests

Adjust only what is actually required by the Helix 2.0.1 Maven build.

The goal is:

    reference/apache-helix/oracle

must compile against the `helix-core` artifact built from our pinned local source tree, not an arbitrary artifact already present in ~/.m2.

Do not fetch or substitute a newer Helix release.

Do not use `main`.

# 16. Oracle Maven module

Create:

    reference/apache-helix/oracle/pom.xml

It should depend on:

    org.apache.helix:helix-core:<the exact version produced by the pinned 2.0.1 tree>

Use the actual group/version found in the upstream POM.

Do not guess if the exact Maven version string differs from `2.0.1`.

Additional dependencies such as Jackson and Mockito are acceptable if needed.

Keep dependencies minimal.

The oracle should be runnable non-interactively from the shell.

# 17. Do not consume Helix's test-JAR during M0 unless strictly necessary

M0 should work against the normal production `helix-core` artifact if possible.

Helix's own test-JAR may be useful later for integration infrastructure such as:

    BaseStageTest
    ZkTestBase
    ClusterControllerManager
    MockParticipantManager

but do not introduce it now unless direct invocation of the production stage is impossible without a tiny test helper.

If you do require the test-JAR during M0, document exactly why.

Do not copy those classes.

# 18. Java oracle shape

Implement a small Java command-line program under:

    reference/apache-helix/oracle/src/main/java/

Use a package local to the oracle, for example:

    org.clustodian.oracle

unless package visibility in Helix makes another package demonstrably necessary.

Keep the adapter small.

A reasonable internal organization is:

    ClustodianOracleMain.java
    ScenarioV1.java
    ResultV1.java
    MessageGenerationOracle.java
    StateModelOracle.java
    CanonicalJson.java

Do not overengineer this structure if fewer classes are cleaner.

The command should conceptually support:

    run-java-oracle.sh <scenario.json>

and print ONLY canonical result JSON to stdout.

Diagnostics/logging must go to stderr.

Exit non-zero on errors.

Do not swallow exceptions.

# 19. Scenario protocol

Create:

    reference/apache-helix/schema/scenario-v1.schema.json

Use JSON.

Keep schema v1 deliberately narrow.

It only needs enough information for the M0 state-model/message-generation oracle.

A conceptual shape is:

    {
      "scenario_version": 1,

      "operation": "generate_transitions",

      "state_model": {
        ...
      },

      "resource": {
        "name": "documents",
        "partitions": [...]
      },

      "instances": [
        ...
      ],

      "current_state": {
        ...
      },

      "target_state": {
        ...
      }
    }

You may use Helix's built-in LeaderStandby state model if that results in a substantially smaller and more faithful first harness.

If so, represent that explicitly in the scenario rather than reproducing the full state model by hand.

Support only what M0 fixtures require.

Do not prematurely model:

    cluster topology
    capacities
    delayed rebalance
    WAGED metadata
    routing tables
    Gateway
    ZooKeeper sessions

# 20. Result protocol

Create:

    reference/apache-helix/schema/result-v1.schema.json

A conceptual transition result should look like:

    {
      "result_schema_version": 1,

      "oracle": {
        "implementation": "apache-helix",
        "helix_version": "...",
        "helix_tag": "helix-2.0.1"
      },

      "transitions": [
        {
          "resource": "documents",
          "partition": "documents_0",
          "instance": "node-b",
          "from": "STANDBY",
          "to": "LEADER",
          "message_type": "STATE_TRANSITION"
        }
      ]
    }

Include exact commit provenance if it can be established reliably from the local reference tree.

Do not invent a commit SHA if the extracted tree contains no Git metadata and it cannot be recovered safely.

If unavailable, record:

    tag = helix-2.0.1

and clearly document that commit-level provenance was unavailable.

# 21. Canonicalization

Do not serialize the complete Helix `Message` object.

Only expose semantics relevant to the claimed behavior.

For M0 transition generation, likely include:

    resource
    partition
    target instance
    from state
    to state
    semantic message type

Do NOT include incidental values such as:

    randomly generated message UUID
    creation timestamp
    Java object identity
    controller instance name
    arbitrary logging data
    incidental HashMap order

unless inspection proves that some field is actually semantically relevant to the behavior we are testing.

Sort transition results deterministically by an explicit semantic tuple such as:

    resource
    partition
    instance
    from
    to

Do not hide semantically meaningful ordering if one exists.

Avoid adding `jq` or another external runtime dependency merely for canonicalization unless the repository already relies on it.

Prefer producing deterministic canonical JSON directly.

# 22. M0 fixture corpus

Create small scenarios under:

    reference/apache-helix/scenarios/m0/

At minimum include cases conceptually equivalent to:

    01-stable.json

        current = STANDBY
        target  = STANDBY

        Expect:
            no state transition


    02-promote.json

        current = STANDBY
        target  = LEADER

        Observe:
            actual Helix transition


    03-demote.json

        current = LEADER
        target  = STANDBY

        Observe:
            actual Helix transition


    04-bootstrap.json

        current = OFFLINE
        target  = STANDBY

        Observe:
            actual Helix transition


    05-multihop.json

        current = OFFLINE
        target  = LEADER

        Important:

        Helix must determine the correct NEXT transition from the actual
        state model.

        Do not hard-code the assumption that OFFLINE transitions directly
        to LEADER.

        This fixture is important because it demonstrates that we are
        exercising real Helix state-machine path semantics.


    06-drop.json

        exercise the applicable OFFLINE/DROPPED path if supported by the
        chosen real state model.

If exact built-in state names differ, use the actual Helix 2.0.1 model.

Do not alter Helix terminology to force the examples above.

# 23. Expected results

Create corresponding files under:

    reference/apache-helix/expected/m0/

Expected output MUST be generated by executing the actual Java oracle.

Do not hand-author transition answers based on our assumptions.

Provide:

    reference/apache-helix/scripts/regenerate-m0-expected.sh

This must be the ONLY ordinary way to regenerate expected results.

It must be explicit.

Verification must never regenerate them automatically.

# 24. verify-m0.sh

Create:

    reference/apache-helix/verify-m0.sh

It must:

1. locate repository root robustly;
2. ensure the pinned Helix reference artifact is built;
3. build the Java oracle;
4. iterate over every `reference/apache-helix/scenarios/m0/*.json`;
5. run the actual Java oracle;
6. write actual output to a temporary directory;
7. compare actual canonical JSON against the checked-in expected result;
8. show a concise useful diff on mismatch;
9. fail non-zero on any difference;
10. leave checked-in expected output untouched.

Example usage:

    ./reference/apache-helix/verify-m0.sh

This command is the M0 acceptance gate.

# 25. Mutation test of the verifier

After generating the fixtures:

1. temporarily modify one semantic field in one expected result;
2. run:

       ./reference/apache-helix/verify-m0.sh

3. prove that verification fails;
4. restore the correct expected file;
5. rerun verification and prove it passes.

Report this explicitly.

This is mandatory.

We want evidence that the measuring instrument can detect a bad result.

# 26. Protect the upstream reference and oracle

Add appropriate guidance, preferably in:

    src/README.md
    reference/apache-helix/README.md

and in AGENTS.md if this repository uses it for agent constraints.

During ordinary Rust implementation milestones, agents MUST NOT modify:

    reference/apache-helix-2.0.1/**
    reference/apache-helix/oracle/**
    reference/apache-helix/schema/**
    reference/apache-helix/scenarios/**
    reference/apache-helix/expected/**
    reference/apache-helix/verify-*.sh

unless the task explicitly says that the conformance harness itself is being changed.

When Rust and Java disagree:

    assume Rust is wrong first.

Do not weaken the oracle.

Do not regenerate expected output merely to make Rust pass.

Any intentional semantic divergence must be explicit, reviewed, and removed from the claimed Helix-compatible subset.

# 27. src/README.md

This README is a major M0 deliverable.

Keep it concise and useful to future coding agents.

It must include:

## Purpose

A standalone Rust implementation of selected Apache Helix cluster-management semantics.

## Scope rule

State prominently:

    standalone in architecture;
    Search-Kernel-driven in feature scope.

Do not port features merely because Helix contains them.

## Clustodian relationship

Explain that Clustodian is the first consumer and determines which generic capabilities are valuable, but no Clustodian-specific type or behavior belongs in this crate.

## Helix relationship

Apache Helix 2.0.1 is the primary semantic and executable reference for Helix-derived behavior.

## Compatibility policy

Compatibility is claimed per feature, never globally.

## Responsibility boundary

Generic Helix/control-plane behavior belongs here.

Clustodian replication, recovery, storage, publication, fencing semantics, distributed query execution, and index lifecycle do not.

## Backend policy

ZooKeeper compatibility is NOT a goal.

ZooKeeper may appear later inside the Java integration oracle because real Apache Helix uses it.

The Rust project must not inherit ZooKeeper's data model or APIs as architectural assumptions.

Do not choose etcd/OpenRaft during M0.

## Testing policy

For Helix-derived features:

    actual Helix 2.0.1
        ->
    executable oracle
        ->
    differential conformance

For generic behavior we later add that is not Helix-derived:

    property/invariant/simulation tests

For Clustodian-specific distributed behavior:

    tests outside clustodian

## Capability matrix

Start with something like:

    Capability                              Status
    ----------------------------------------------------------------
    StateModelDefinition oracle             M0 oracle
    transition-generation oracle            M0 oracle
    Rust StateModelDefinition               not implemented
    Rust transition generation              not implemented
    BestPossibleState                       not implemented
    basic replica placement                 not implemented
    automatic rebalance                     not implemented
    node-loss rebalance                     not implemented
    topology-aware placement                not implemented
    transition throttling                   not implemented
    delayed rebalance                       out until needed
    WAGED                                   out until needed
    Task Framework                          out of scope
    ZooKeeper compatibility                 out of scope
    Java API compatibility                  out of scope
    REST admin compatibility                out of scope

Update exact names/statuses based on what M0 actually proves.

Never mark behavior implemented based solely on having a Java oracle.

# 28. reference/apache-helix/README.md

Document the conformance laboratory itself.

Include:

- pinned reference: Apache Helix tag `helix-2.0.1`;
- location of pristine source;
- how the isolated Maven repository works;
- Java oracle architecture;
- exact real Helix classes invoked;
- why M0 is ZooKeeper-free;
- scenario/result schema versions;
- expected-result generation;
- verification command;
- distinction between M0 stage oracle and future integration oracle.

Explain the future testing ladder:

    M0:
        in-process state model + MessageGenerationPhase oracle
        no ZooKeeper

    later:
        BestPossibleState/rebalancer oracle

    later:
        placement/rebalance differential fuzzing

    later:
        full convergence oracle using real controller +
        participants + ZooKeeper test infrastructure

Do not imply that M0 proves cluster convergence.

# 29. No Quickwit dependency

Do not use Quickwit code in M0.

Quickwit may remain an external architectural reference later, but Apache Helix is the executable reference for this project's Helix-derived semantics.

Do not introduce Quickwit crates or source.

# 30. No ZooKeeper abstraction yet

Do NOT create a speculative Rust trait such as:

    ClusterMetadataStore
    CoordinationBackend
    ConsensusStore

during M0.

We have not yet reached the behavior that requires that abstraction.

Do not design around:

    ZooKeeper
    etcd
    OpenRaft

yet.

The correct abstraction should emerge later from the actual controller functionality we choose to implement.

# 31. Do not implement Rust Helix behavior

This is non-negotiable.

M0 must NOT implement:

    StateModelDefinition behavior in Rust
    state transition traversal in Rust
    MessageGenerationPhase in Rust
    controller stages
    placement
    rebalancing
    participants
    routing
    Gateway
    metadata backend
    membership
    etcd
    OpenRaft

The Rust crate may contain:

    crate-level documentation
    project boundary documentation
    minimal placeholder/version metadata

but no substantive cluster-management algorithm.

The purpose of M0 is to finish the measuring instrument BEFORE implementing what it measures.

# 32. Code quality

Treat this as foundational distributed-systems infrastructure.

Requirements:

- no warnings;
- no ignored build failures;
- no swallowed Java exceptions;
- deterministic output;
- scripts use `set -euo pipefail`;
- shell scripts work when invoked from repository root;
- avoid absolute machine-specific paths;
- keep generated build output out of Git;
- add appropriate `.gitignore` entries for:
      reference/apache-helix/.m2/
      Maven build directories
      temporary oracle output
  while preserving checked-in expected fixtures;
- do not mutate the pristine Helix tree;
- do not fetch source code at runtime;
- do not silently switch Helix versions;
- do not add speculative framework architecture.

# 33. Verify the upstream tree remains pristine

Before finishing, verify that this task has not modified:

    reference/apache-helix-2.0.1/

If it is a Git checkout, use Git status/diff.

If it is only an extracted tree, use a before/after manifest/hash approach for files touched by the task or another reliable method.

Report how you verified this.

# 34. M0 exit criteria

M0 is complete only when ALL of the following are true:

- `src/` exists as an independent Rust crate.
- Its README prominently states:
      standalone architecture;
      Search-Kernel-driven scope.
- No Clustodian dependency exists in `clustodian`.
- No substantive Rust Helix implementation exists yet.
- `reference/apache-helix-2.0.1/` is unchanged.
- A standalone Java oracle module exists.
- It compiles against `helix-core` built from the pinned local source.
- The Helix artifact is isolated from the user's global Maven cache.
- The oracle invokes actual Apache Helix production code.
- The oracle exercises actual StateModelDefinition semantics.
- The oracle exercises actual `MessageGenerationPhase`.
- The oracle contains no recreated transition algorithm.
- M0 requires no ZooKeeper.
- Versioned scenario and result schemas exist.
- Multiple small M0 scenarios exist.
- Expected outputs were generated by the real Helix oracle.
- `reference/apache-helix/verify-m0.sh` passes.
- Corrupting an expected result causes `verify-m0.sh` to fail.
- Restoring the fixture causes it to pass again.
- The README accurately states what is and is not proven.

# 35. What M0 proves

M0 should be able to truthfully claim something approximately like:

    We can execute selected Apache Helix 2.0.1 state-model and
    transition-generation behavior directly from the pinned Java
    implementation and capture deterministic language-neutral oracle
    results suitable for future Rust differential testing.

It must NOT claim:

    clustodian implements Helix;
    replica placement is correct;
    rebalance is correct;
    controller convergence is correct;
    failure handling is correct;
    Gateway compatibility exists;
    ZooKeeper has been replaced.

# 36. Recommended next milestones

Do NOT implement these now, but use this intended sequence when recommending M1.

## M1 — Rust state model

Implement only the Rust equivalents needed for:

    State
    StateModelDefinition
    transition graph / next-hop behavior

Compare against the M0 StateModel oracle.

## M2 — Rust transition generation

Implement the Rust equivalent of the subset exercised through:

    MessageGenerationPhase

Given:

    CurrentState
    target state
    StateModelDefinition

produce semantically equivalent transition requests.

Compare against actual Helix.

## M3 — Best-possible state without automatic placement

Prefer beginning with explicit/preference-list or SEMI_AUTO-style input so we can separate:

    desired assignment/state calculation

from:

    automatic placement

Do not introduce both problems in the same milestone.

## M4 — First automatic placement/rebalance policy

Select the specific Helix 2.0.1 algorithm that matches Clustodian's actual requirements.

Only then add:

    node joins
    node loss
    movement minimization
    failure-domain placement

with differential testing.

## Later — full integration/convergence oracle

Only when needed, introduce Java-side Helix test infrastructure such as the actual 2.0.1 equivalents of:

    ZkTestBase
    ClusterControllerManager
    MockParticipantManager
    BestPossibleExternalViewVerifier
    ZkHelixClusterVerifier

At that stage ZooKeeper is permitted INSIDE THE JAVA ORACLE ONLY.

It does not imply a ZooKeeper backend for clustodian.

# 37. Final response required

When finished, give a concise engineering report containing:

1. files created/modified;
2. exact Apache Helix 2.0.1 classes invoked by the oracle;
3. exact upstream Helix tests used as examples for invocation;
4. exact Maven version resolved from the pinned tree;
5. exact isolated Maven repository path;
6. whether the normal `helix-core` artifact was sufficient or a test-JAR was needed;
7. confirmation that M0 uses no ZooKeeper;
8. scenario schema summary;
9. result schema summary;
10. fixture list;
11. exact command:
        ./reference/apache-helix/verify-m0.sh
12. verification result;
13. mutation-test result proving the verifier catches a wrong fixture;
14. how `reference/apache-helix-2.0.1/` was verified unchanged;
15. what M0 now proves;
16. what M0 explicitly does NOT prove;
17. recommended M1 scope.

Do not implement M1.
