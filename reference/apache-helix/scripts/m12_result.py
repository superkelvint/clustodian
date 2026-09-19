#!/usr/bin/env python3
"""Shared validation and normalization for M12 result documents."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_json(path: str) -> Any:
    with Path(path).open(encoding="utf-8") as handle:
        return json.load(handle)


def expected_checkpoint_names(scenario: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for index, step in enumerate(scenario.get("steps", [])):
        if not isinstance(step, dict):
            raise ValueError(f"scenario step {index} is not an object")
        if step.get("op") != "checkpoint":
            continue
        name = step.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(f"scenario checkpoint {index} has no non-empty name")
        if name in names:
            raise ValueError(f"scenario contains duplicate checkpoint {name!r}")
        names.append(name)
    return names


def _require_object(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be an object")
    return value


def _require_list(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{path} must be a list")
    return value


def _require_string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{path} must be a non-empty string")
    return value


def _expected_started_participants(scenario: dict[str, Any]) -> set[str]:
    started: set[str] = set()
    for step in scenario.get("steps", []):
        if not isinstance(step, dict):
            continue
        if step.get("op") == "start_participants":
            started.update(step.get("instances", []))
        elif step.get("op") in {"start_participant", "restart_participant"}:
            started.add(step.get("instance"))
    return {instance for instance in started if isinstance(instance, str)}


def _validate_process_evidence(result: dict[str, Any], scenario: dict[str, Any]) -> None:
    evidence = _require_object(result.get("process_evidence"), "process_evidence")
    controllers = _require_object(evidence.get("controllers"), "process_evidence.controllers")
    participants = _require_object(evidence.get("participants"), "process_evidence.participants")

    expected_controllers = {
        _require_string(controller, "scenario.cluster.controllers[]")
        for controller in scenario.get("cluster", {}).get("controllers", [])
    }
    if not expected_controllers:
        raise ValueError("scenario has no configured controllers")
    if set(controllers) != expected_controllers:
        raise ValueError(
            "process_evidence.controllers does not cover exactly the configured controllers"
        )
    if not _expected_started_participants(scenario).issubset(participants):
        raise ValueError("process_evidence.participants omits a started participant")

    pids: list[int] = []
    for process_type, processes in (
        ("controllers", controllers),
        ("participants", participants),
    ):
        for process, pid in processes.items():
            if not isinstance(process, str) or not process:
                raise ValueError(f"process_evidence.{process_type} has an invalid process id")
            if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
                raise ValueError(f"process_evidence.{process_type}.{process} has an invalid PID")
            pids.append(pid)
    if len(pids) != len(set(pids)):
        raise ValueError("process evidence contains duplicate PIDs")


def _expected_partitions(resource: dict[str, Any]) -> set[str]:
    if resource.get("rebalance") == "SEMI_AUTO":
        preference_lists = _require_object(
            resource.get("preference_lists"),
            f"resource {resource.get('name')!r}.preference_lists",
        )
        return set(preference_lists)
    name = _require_string(resource.get("name"), "resource.name")
    partitions = resource.get("partitions")
    if not isinstance(partitions, int) or isinstance(partitions, bool) or partitions <= 0:
        raise ValueError(f"resource {name!r} has an invalid partition count")
    return {f"{name}_{index}" for index in range(partitions)}


def _validate_checkpoint(
    checkpoint: dict[str, Any],
    name: str,
    scenario: dict[str, Any],
    settled: bool,
) -> None:
    if checkpoint.get("name") != name:
        raise ValueError(f"checkpoint order/name mismatch: expected {name!r}")

    controllers = _require_object(checkpoint.get("controllers"), f"{name}.controllers")
    active = _require_list(controllers.get("active"), f"{name}.controllers.active")
    standby = _require_list(controllers.get("standby"), f"{name}.controllers.standby")
    if len(active) != 1:
        raise ValueError(f"{name}: expected exactly one active controller")
    active_ids = {_require_string(value, f"{name}.controllers.active[]") for value in active}
    standby_ids = {_require_string(value, f"{name}.controllers.standby[]") for value in standby}
    if active_ids & standby_ids:
        raise ValueError(f"{name}: active controller also appears in standby")
    configured_controllers = {
        _require_string(value, "scenario.cluster.controllers[]")
        for value in scenario.get("cluster", {}).get("controllers", [])
    }
    if not active_ids | standby_ids <= configured_controllers:
        raise ValueError(f"{name}: result contains an unknown controller")

    live = _require_object(checkpoint.get("live_instances"), f"{name}.live_instances")
    for instance, session in live.items():
        _require_string(instance, f"{name}.live_instances key")
        _require_string(session, f"{name}.live_instances.{instance}")

    current_state = _require_object(
        checkpoint.get("active_current_state"), f"{name}.active_current_state"
    )
    for instance, state in current_state.items():
        _require_string(instance, f"{name}.active_current_state key")
        state = _require_object(state, f"{name}.active_current_state.{instance}")
        session = _require_string(
            state.get("session"), f"{name}.active_current_state.{instance}.session"
        )
        if instance not in live:
            raise ValueError(f"{name}: CurrentState for dead participant {instance}")
        if live[instance] != session:
            raise ValueError(f"{name}: CurrentState session does not match live session for {instance}")
        _require_object(state.get("resources"), f"{name}.active_current_state.{instance}.resources")

    external_view = _require_object(checkpoint.get("external_view"), f"{name}.external_view")
    pending = _require_list(checkpoint.get("pending_transitions"), f"{name}.pending_transitions")
    for index, transition in enumerate(pending):
        transition = _require_object(transition, f"{name}.pending_transitions[{index}]")
        instance = _require_string(
            transition.get("instance"), f"{name}.pending_transitions[{index}].instance"
        )
        target_session = _require_string(
            transition.get("target_session"),
            f"{name}.pending_transitions[{index}].target_session",
        )
        if live.get(instance) != target_session:
            raise ValueError(f"{name}: transition targets a stale session")
    if settled and pending:
        raise ValueError(f"{name}: settled checkpoint still has pending transitions")

    routing = _require_list(checkpoint.get("routing_results"), f"{name}.routing_results")
    for index, result in enumerate(routing):
        result = _require_object(result, f"{name}.routing_results[{index}]")
        for field in ("resource", "partition", "state"):
            _require_string(result.get(field), f"{name}.routing_results[{index}].{field}")
        instances = _require_list(
            result.get("instances"), f"{name}.routing_results[{index}].instances"
        )
        for instance in instances:
            _require_string(instance, f"{name}.routing_results[{index}].instances[]")

    if not settled:
        return

    resources = scenario.get("cluster", {}).get("resources", [])
    expected_resources = {
        _require_string(resource.get("name"), "resource.name") for resource in resources
    }
    if set(external_view) != expected_resources:
        raise ValueError(f"{name}: ExternalView resource set does not match the scenario")
    configured_instances = {
        _require_string(instance.get("id"), "participant.id")
        for instance in scenario.get("cluster", {}).get("participants", [])
    }
    for resource in resources:
        resource_name = resource["name"]
        partitions = _require_object(
            external_view[resource_name], f"{name}.external_view.{resource_name}"
        )
        if set(partitions) != _expected_partitions(resource):
            raise ValueError(
                f"{name}: partition set for {resource_name!r} is incomplete or unexpected"
            )
        replicas = resource.get("replicas")
        if not isinstance(replicas, int) or isinstance(replicas, bool) or replicas <= 0:
            raise ValueError(f"resource {resource_name!r} has an invalid replica count")
        for partition, states in partitions.items():
            states = _require_object(states, f"{name}.external_view.{resource_name}.{partition}")
            leaders = []
            assigned = []
            for instance, state in states.items():
                if instance not in configured_instances:
                    raise ValueError(f"{name}: unknown instance {instance!r} in ExternalView")
                state = _require_string(
                    state, f"{name}.external_view.{resource_name}.{partition}.{instance}"
                )
                if instance in live and state in {"LEADER", "STANDBY"}:
                    assigned.append(instance)
                    if state == "LEADER":
                        leaders.append(instance)
            if len(leaders) > 1:
                raise ValueError(f"{name}: {resource_name}/{partition} has multiple live leaders")
            if assigned and len(leaders) != 1:
                raise ValueError(f"{name}: {resource_name}/{partition} lacks one live leader")
            if len(live) >= replicas and len(assigned) != replicas:
                raise ValueError(
                    f"{name}: {resource_name}/{partition} has {len(assigned)} replicas, expected {replicas}"
                )

    for result in routing:
        resource_name = result["resource"]
        partition = result["partition"]
        state = result["state"]
        actual = {
            instance
            for instance, value in external_view[resource_name][partition].items()
            if instance in live and value == state
        }
        if set(result["instances"]) != actual:
            raise ValueError(f"{name}: routing result disagrees with ExternalView")


def validate_result(result: Any, scenario: dict[str, Any]) -> None:
    result = _require_object(result, "result")
    if result.get("result_schema_version") != 1:
        raise ValueError("result_schema_version must be 1")
    expected_name = _require_string(scenario.get("name"), "scenario.name")
    if result.get("scenario") != expected_name:
        raise ValueError("result scenario does not match scenario input")
    _validate_process_evidence(result, scenario)

    checkpoints = _require_list(result.get("checkpoints"), "checkpoints")
    names = expected_checkpoint_names(scenario)
    if len(checkpoints) != len(names):
        raise ValueError(f"expected {len(names)} checkpoints, got {len(checkpoints)}")
    settled = set(scenario.get("expectations", {}).get("settled_checkpoints", []))
    if not settled <= set(names):
        raise ValueError("scenario expectations reference an unknown settled checkpoint")
    for name, checkpoint in zip(names, checkpoints):
        _validate_checkpoint(
            _require_object(checkpoint, f"checkpoint {name!r}"), name, scenario, name in settled
        )

    events = _require_list(result.get("controller_events"), "controller_events")
    for index, event in enumerate(events):
        event = _require_object(event, f"controller_events[{index}]")
        _require_string(event.get("kind"), f"controller_events[{index}].kind")
        for field in (
            "new_controller_elected",
            "old_controller_fenced",
            "stale_publish_rejected",
            "restarted_controller_did_not_preempt",
        ):
            if field in event and not isinstance(event[field], bool):
                raise ValueError(f"controller_events[{index}].{field} must be boolean")

    expectations = _require_object(scenario.get("expectations"), "scenario.expectations")
    minimum_failovers = expectations.get("min_controller_failovers", 0)
    failovers = sum(
        event.get("kind") in {"leader_crash", "lease_loss", "controller_failover"}
        and event.get("new_controller_elected") is True
        for event in events
    )
    if failovers < minimum_failovers:
        raise ValueError(f"expected at least {minimum_failovers} observed failovers, got {failovers}")
    if expectations.get("require_stale_controller_fence") and not any(
        event.get("old_controller_fenced") is True
        and event.get("stale_publish_rejected") is True
        for event in events
    ):
        raise ValueError("no stale-controller fence evidence")
    if expectations.get("require_session_replacement"):
        sessions: dict[str, str] = {}
        replaced = False
        for checkpoint in checkpoints:
            for instance, session in checkpoint["live_instances"].items():
                if instance in sessions and sessions[instance] != session:
                    replaced = True
                sessions[instance] = session
        if not replaced:
            raise ValueError("participant session replacement was not observed")


def canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: canonical(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [canonical(item) for item in value]
    return value


def normalized_result(result: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    validate_result(result, scenario)
    settled = set(scenario["expectations"].get("settled_checkpoints", []))
    checkpoints = []
    for checkpoint in result["checkpoints"]:
        controllers = checkpoint["controllers"]
        normalized = {
            "name": checkpoint["name"],
            "controller_active_count": len(controllers["active"]),
            "controller_standby_count": len(controllers["standby"]),
            "live_instances": canonical(checkpoint["live_instances"]),
        }
        if checkpoint["name"] in settled:
            normalized.update(
                {
                    "active_current_state": canonical(checkpoint["active_current_state"]),
                    "external_view": canonical(checkpoint["external_view"]),
                    "pending_transitions": canonical(checkpoint["pending_transitions"]),
                    "routing_results": canonical(checkpoint["routing_results"]),
                }
            )
        else:
            normalized["has_pending_transitions"] = bool(checkpoint["pending_transitions"])
        checkpoints.append(normalized)

    event_counts: dict[tuple[Any, ...], int] = {}
    for event in result["controller_events"]:
        key = (
            event["kind"],
            event.get("old_controller_fenced", False),
            event.get("stale_publish_rejected", False),
            event.get("new_controller_elected", False),
            event.get("restarted_controller_did_not_preempt", False),
        )
        event_counts[key] = event_counts.get(key, 0) + 1
    return {
        "scenario": result["scenario"],
        "checkpoints": checkpoints,
        "events": sorted([list(key) + [count] for key, count in event_counts.items()]),
    }
