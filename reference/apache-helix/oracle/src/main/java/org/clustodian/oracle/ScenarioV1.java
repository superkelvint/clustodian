package org.clustodian.oracle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class ScenarioV1 {
  static final ObjectMapper MAPPER = new ObjectMapper();

  final String operation;
  final String stateModelName;
  final String resourceName;
  final List<String> partitions;
  final List<InstanceSpec> instances;
  final int replicas;
  final List<String> liveInstances;
  final Map<String, List<String>> preferenceLists;
  final Map<String, Map<String, String>> currentState;
  final Map<String, Map<String, String>> targetState;
  final List<StateQuery> stateQueries;
  final List<StateCount> stateCounts;
  final List<TransitionSpec> candidateTransitions;
  final List<TransitionSpec> pendingTransitions;
  final int maxPartitionsPerInstance;
  final TopologySpec topology;
  final List<M6ResourceSpec> m6Resources;
  final List<M6ThrottleConfig> m6ThrottleConfigs;
  final List<M6TransitionSpec> m6PendingTransitions;
  final List<String> m6LiveInstances;
  final List<M7ResourceSpec> m7Resources;
  final List<String> m7Instances;
  final List<M7RoutingQuery> m7RoutingQueries;

  private ScenarioV1(String operation, String stateModelName, String resourceName,
      List<String> partitions, List<InstanceSpec> instances,
      int replicas, List<String> liveInstances, Map<String, List<String>> preferenceLists,
      Map<String, Map<String, String>> currentState,
      Map<String, Map<String, String>> targetState, List<StateQuery> stateQueries,
      List<StateCount> stateCounts, List<TransitionSpec> candidateTransitions,
      List<TransitionSpec> pendingTransitions, int maxPartitionsPerInstance,
      TopologySpec topology) {
    this(operation, stateModelName, resourceName, partitions, instances, replicas, liveInstances,
        preferenceLists, currentState, targetState, stateQueries, stateCounts,
        candidateTransitions, pendingTransitions, maxPartitionsPerInstance, topology,
        List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
  }

  private ScenarioV1(String operation, String stateModelName, String resourceName,
      List<String> partitions, List<InstanceSpec> instances,
      int replicas, List<String> liveInstances, Map<String, List<String>> preferenceLists,
      Map<String, Map<String, String>> currentState,
      Map<String, Map<String, String>> targetState, List<StateQuery> stateQueries,
      List<StateCount> stateCounts, List<TransitionSpec> candidateTransitions,
      List<TransitionSpec> pendingTransitions, int maxPartitionsPerInstance,
      TopologySpec topology, List<M6ResourceSpec> m6Resources,
      List<M6ThrottleConfig> m6ThrottleConfigs, List<M6TransitionSpec> m6PendingTransitions,
      List<String> m6LiveInstances, List<M7ResourceSpec> m7Resources,
      List<String> m7Instances, List<M7RoutingQuery> m7RoutingQueries) {
    this.operation = operation;
    this.stateModelName = stateModelName;
    this.resourceName = resourceName;
    this.partitions = partitions;
    this.instances = instances;
    this.replicas = replicas;
    this.liveInstances = liveInstances;
    this.preferenceLists = preferenceLists;
    this.currentState = currentState;
    this.targetState = targetState;
    this.stateQueries = stateQueries;
    this.stateCounts = stateCounts;
    this.candidateTransitions = candidateTransitions;
    this.pendingTransitions = pendingTransitions;
    this.maxPartitionsPerInstance = maxPartitionsPerInstance;
    this.topology = topology;
    this.m6Resources = m6Resources;
    this.m6ThrottleConfigs = m6ThrottleConfigs;
    this.m6PendingTransitions = m6PendingTransitions;
    this.m6LiveInstances = m6LiveInstances;
    this.m7Resources = m7Resources;
    this.m7Instances = m7Instances;
    this.m7RoutingQueries = m7RoutingQueries;
  }

  static ScenarioV1 read(Path path) throws IOException {
    JsonNode root = MAPPER.readTree(path.toFile());
    requireObject(root, "scenario");
    if (requiredInt(root, "scenario_version") != 1) {
      throw invalid("scenario_version must be 1");
    }

    String operation = requiredText(root, "operation");
    if (!operation.equals("generate_transitions")
        && !operation.equals("inspect_state_model")
        && !operation.equals("compute_semi_auto_best_possible")
        && !operation.equals("compute_crush_assignment")
        && !operation.equals("select_transitions")
        && !operation.equals("compute_intermediate_and_throttle")
        && !operation.equals("compute_external_view_and_routing")) {
      throw invalid("unsupported operation: " + operation);
    }

    if (operation.equals("compute_crush_assignment")) {
      return readCrushScenario(root);
    }

    if (operation.equals("compute_external_view_and_routing")) {
      return readM7Scenario(root);
    }

    JsonNode stateModel = requiredObject(root, "state_model");
    if (!requiredText(stateModel, "kind").equals("built_in")) {
      throw invalid("M0 supports only built_in state models");
    }
    String stateModelName = requiredText(stateModel, "name");
    if (!stateModelName.equals("LeaderStandby")) {
      throw invalid("M0 supports only the LeaderStandby state model");
    }

    if (operation.equals("compute_intermediate_and_throttle")) {
      return readM6Scenario(root, stateModelName);
    }

    if (operation.equals("inspect_state_model")) {
      return new ScenarioV1(operation, stateModelName, null, List.of(), List.of(),
          0, List.of(), Map.of(),
          Map.of(), Map.of(), readQueries(root), List.of(), List.of(), List.of(), -1, null);
    }

    JsonNode resource = requiredObject(root, "resource");
    String resourceName = requiredText(resource, "name");
    List<String> partitions = requiredTextArray(resource, "partitions");
    if (partitions.isEmpty()) {
      throw invalid("resource.partitions must not be empty");
    }

    List<InstanceSpec> instances = readInstances(root);
    if (instances.isEmpty()) {
      throw invalid("instances must not be empty");
    }
    if (operation.equals("compute_semi_auto_best_possible")) {
      int replicas = requiredInt(root, "replicas");
      List<String> liveInstances = requiredTextArray(root, "live_instances");
      Map<String, List<String>> preferenceLists = readPreferenceLists(root);
      Map<String, Map<String, String>> currentState = readStateMap(root, "current_state");
      validateStateMap(currentState, partitions, instances, "current_state");
      return new ScenarioV1(operation, stateModelName, resourceName, partitions, instances,
          replicas, liveInstances, preferenceLists, currentState, Map.of(), List.of(),
          List.of(), List.of(), List.of(), -1, null);
    }

    if (operation.equals("select_transitions")) {
      int replicas = requiredInt(root, "replicas");
      List<String> liveInstances = requiredTextArray(root, "live_instances");
      Map<String, List<String>> preferenceLists = readPreferenceLists(root);
      Map<String, Map<String, String>> currentState = readStateMap(root, "current_state");
      validateStateMap(currentState, partitions, instances, "current_state");
      List<TransitionSpec> candidateTransitions = readTransitions(root, "candidate_transitions");
      List<TransitionSpec> pendingTransitions = readTransitions(root, "pending_transitions");
      validateTransitions(candidateTransitions, partitions, instances, "candidate_transitions");
      validateTransitions(pendingTransitions, partitions, instances, "pending_transitions");
      return new ScenarioV1(operation, stateModelName, resourceName, partitions, instances,
          replicas, liveInstances, preferenceLists, currentState, Map.of(), List.of(),
          List.of(), candidateTransitions, pendingTransitions, -1, null);
    }

    Map<String, Map<String, String>> currentState = readStateMap(root, "current_state");
    Map<String, Map<String, String>> targetState = readStateMap(root, "target_state");
    validateStateMap(currentState, partitions, instances, "current_state");
    validateStateMap(targetState, partitions, instances, "target_state");

    return new ScenarioV1(operation, stateModelName, resourceName, partitions, instances,
        0, List.of(), Map.of(), currentState, targetState, List.of(), List.of(), List.of(),
        List.of(), -1, null);
  }

  private static ScenarioV1 readCrushScenario(JsonNode root) {
    String resourceName = requiredText(root, "resource");
    List<String> partitions = requiredTextArray(root, "partitions");
    if (partitions.isEmpty()) {
      throw invalid("partitions must not be empty");
    }
    List<InstanceSpec> instances = readCrushInstances(root);
    if (instances.isEmpty()) {
      throw invalid("instances must not be empty");
    }
    List<StateCount> stateCounts = readStateCounts(root);
    int maxPartitionsPerInstance = requiredInt(root, "max_partitions_per_instance");
    if (maxPartitionsPerInstance != -1) {
      throw invalid("M4 supports only max_partitions_per_instance = -1");
    }
    List<String> liveInstances = requiredTextArray(root, "live_instances");
    TopologySpec topology = readTopology(root);
    return new ScenarioV1("compute_crush_assignment", null, resourceName, partitions, instances,
        0, liveInstances, Map.of(), Map.of(), Map.of(), List.of(), stateCounts,
        List.of(), List.of(), maxPartitionsPerInstance, topology);
  }

  private static Map<String, List<String>> readPreferenceLists(JsonNode root) {
    JsonNode object = requiredObject(root, "preference_lists");
    Map<String, List<String>> result = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> iterator = object.fields();
    while (iterator.hasNext()) {
      Map.Entry<String, JsonNode> entry = iterator.next();
      result.put(entry.getKey(), requiredTextArray(object, entry.getKey()));
    }
    return result;
  }

  private static List<InstanceSpec> readInstances(JsonNode root) {
    JsonNode array = requiredArray(root, "instances");
    List<InstanceSpec> instances = new ArrayList<>();
    Set<String> names = new HashSet<>();
    for (JsonNode node : array) {
      requireObject(node, "instance");
      String name = requiredText(node, "name");
      String sessionId = requiredText(node, "session_id");
      if (!names.add(name)) {
        throw invalid("duplicate instance: " + name);
      }
      instances.add(new InstanceSpec(name, sessionId, Map.of()));
    }
    return instances;
  }

  private static List<InstanceSpec> readCrushInstances(JsonNode root) {
    JsonNode array = requiredArray(root, "instances");
    List<InstanceSpec> instances = new ArrayList<>();
    Set<String> names = new HashSet<>();
    for (JsonNode node : array) {
      requireObject(node, "instance");
      String name = requiredText(node, "name");
      JsonNode domainNode = requiredObject(node, "domain");
      Map<String, String> domain = new LinkedHashMap<>();
      Iterator<Map.Entry<String, JsonNode>> fields = domainNode.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        if (!field.getValue().isTextual() || field.getValue().textValue().isEmpty()) {
          throw invalid("instance domain values must be non-empty strings");
        }
        domain.put(field.getKey(), field.getValue().textValue());
      }
      if (!names.add(name)) {
        throw invalid("duplicate instance: " + name);
      }
      instances.add(new InstanceSpec(name, null, domain));
    }
    return instances;
  }

  private static List<StateCount> readStateCounts(JsonNode root) {
    JsonNode array = requiredArray(root, "state_counts");
    List<StateCount> counts = new ArrayList<>();
    for (JsonNode node : array) {
      requireObject(node, "state count");
      counts.add(new StateCount(requiredText(node, "state"), requiredInt(node, "count")));
    }
    return counts;
  }

  private static TopologySpec readTopology(JsonNode root) {
    JsonNode topology = requiredObject(root, "topology");
    return new TopologySpec(requiredText(topology, "path"),
        requiredText(topology, "fault_zone_type"), requiredText(topology, "end_node_type"));
  }

  private static List<StateQuery> readQueries(JsonNode root) {
    JsonNode array = requiredArray(root, "queries");
    List<StateQuery> queries = new ArrayList<>();
    for (JsonNode node : array) {
      requireObject(node, "state query");
      queries.add(new StateQuery(requiredText(node, "from"), requiredText(node, "to")));
    }
    return queries;
  }

  private static List<TransitionSpec> readTransitions(JsonNode root, String field) {
    JsonNode array = requiredArray(root, field);
    List<TransitionSpec> transitions = new ArrayList<>();
    for (JsonNode node : array) {
      requireObject(node, field + " transition");
      transitions.add(new TransitionSpec(requiredText(node, "partition"),
          requiredText(node, "instance"), requiredText(node, "from"),
          requiredText(node, "to")));
    }
    return transitions;
  }

  private static void validateTransitions(List<TransitionSpec> transitions,
      List<String> partitions, List<InstanceSpec> instances, String field) {
    Set<String> validPartitions = new HashSet<>(partitions);
    Set<String> validInstances = new HashSet<>();
    for (InstanceSpec instance : instances) {
      validInstances.add(instance.name);
    }
    for (TransitionSpec transition : transitions) {
      if (!validPartitions.contains(transition.partition)) {
        throw invalid(field + " contains unknown partition: " + transition.partition);
      }
      if (!validInstances.contains(transition.instance)) {
        throw invalid(field + " contains unknown instance: " + transition.instance);
      }
    }
  }

  private static Map<String, Map<String, String>> readStateMap(JsonNode root, String field) {
    return readStateMapNode(requiredObject(root, field), field);
  }

  private static Map<String, Map<String, String>> readStateMapNode(JsonNode partitions,
      String field) {
    Map<String, Map<String, String>> result = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> partitionIterator = partitions.fields();
    while (partitionIterator.hasNext()) {
      Map.Entry<String, JsonNode> partitionEntry = partitionIterator.next();
      JsonNode instances = partitionEntry.getValue();
      requireObject(instances, field + " partition");
      Map<String, String> states = new LinkedHashMap<>();
      Iterator<Map.Entry<String, JsonNode>> instanceIterator = instances.fields();
      while (instanceIterator.hasNext()) {
        Map.Entry<String, JsonNode> instanceEntry = instanceIterator.next();
        if (!instanceEntry.getValue().isTextual()) {
          throw invalid(field + " state must be a string");
        }
        states.put(instanceEntry.getKey(), instanceEntry.getValue().textValue());
      }
      result.put(partitionEntry.getKey(), states);
    }
    return result;
  }

  private static ScenarioV1 readM6Scenario(JsonNode root, String stateModelName) {
    List<String> liveInstances = requiredTextArray(root, "live_instances");
    List<M6ResourceSpec> resources = new ArrayList<>();
    JsonNode resourceArray = requiredArray(root, "resources");
    for (JsonNode resourceNode : resourceArray) {
      requireObject(resourceNode, "M6 resource");
      String name = requiredText(resourceNode, "name");
      int replicas = requiredInt(resourceNode, "replicas");
      int minActiveReplicas = requiredInt(resourceNode, "min_active_replicas");
      JsonNode preferenceNode = requiredObject(resourceNode, "preference_lists");
      Map<String, List<String>> preferenceLists = new LinkedHashMap<>();
      Iterator<Map.Entry<String, JsonNode>> preferenceIterator = preferenceNode.fields();
      while (preferenceIterator.hasNext()) {
        Map.Entry<String, JsonNode> entry = preferenceIterator.next();
        preferenceLists.put(entry.getKey(), requiredTextArray(preferenceNode, entry.getKey()));
      }
      resources.add(new M6ResourceSpec(name, replicas, minActiveReplicas, preferenceLists,
          readStateMapNode(requiredObject(resourceNode, "current_state"), "current_state"),
          readStateMapNode(requiredObject(resourceNode, "best_possible_state"),
              "best_possible_state"),
          readM6Transitions(requiredArray(resourceNode, "selected_transitions"))));
    }

    List<M6ThrottleConfig> throttleConfigs = new ArrayList<>();
    JsonNode throttleArray = requiredArray(root, "throttle_configs");
    for (JsonNode configNode : throttleArray) {
      requireObject(configNode, "M6 throttle config");
      throttleConfigs.add(new M6ThrottleConfig(requiredText(configNode, "rebalance_type"),
          requiredText(configNode, "scope"), requiredInt(configNode, "max_transitions")));
    }

    return new ScenarioV1("compute_intermediate_and_throttle", stateModelName, null,
        List.of(), List.of(), 0, List.of(), Map.of(), Map.of(), Map.of(), List.of(), List.of(),
        List.of(), List.of(), -1, null, resources, throttleConfigs,
        readM6Transitions(requiredArray(root, "pending_transitions")), liveInstances,
        List.of(), List.of(), List.of());
  }

  private static ScenarioV1 readM7Scenario(JsonNode root) {
    List<String> instances = requiredTextArray(root, "instances");
    Set<String> instanceNames = new HashSet<>(instances);
    if (instanceNames.size() != instances.size()) {
      throw invalid("duplicate instance");
    }

    List<M7ResourceSpec> resources = new ArrayList<>();
    JsonNode resourceArray = requiredArray(root, "resources");
    Set<String> resourceNames = new HashSet<>();
    for (JsonNode resourceNode : resourceArray) {
      requireObject(resourceNode, "M7 resource");
      String name = requiredText(resourceNode, "name");
      if (!resourceNames.add(name)) {
        throw invalid("duplicate resource: " + name);
      }
      resources.add(new M7ResourceSpec(name,
          readStateMapNode(requiredObject(resourceNode, "current_state"), "current_state")));
    }
    if (resources.isEmpty()) {
      throw invalid("resources must not be empty");
    }

    List<M7RoutingQuery> routingQueries = new ArrayList<>();
    for (JsonNode queryNode : requiredArray(root, "routing_queries")) {
      requireObject(queryNode, "M7 routing query");
      routingQueries.add(new M7RoutingQuery(requiredText(queryNode, "id"),
          requiredText(queryNode, "resource"), requiredText(queryNode, "partition"),
          requiredText(queryNode, "state")));
    }

    return new ScenarioV1("compute_external_view_and_routing", null, null, List.of(), List.of(),
        0, List.of(), Map.of(), Map.of(), Map.of(), List.of(), List.of(), List.of(), List.of(),
        -1, null, List.of(), List.of(), List.of(), List.of(), resources, instances,
        routingQueries);
  }

  private static List<M6TransitionSpec> readM6Transitions(JsonNode array) {
    List<M6TransitionSpec> transitions = new ArrayList<>();
    for (JsonNode node : array) {
      requireObject(node, "M6 transition");
      transitions.add(new M6TransitionSpec(requiredText(node, "resource"),
          requiredText(node, "partition"), requiredText(node, "instance"),
          requiredText(node, "from"), requiredText(node, "to")));
    }
    return transitions;
  }

  private static void validateStateMap(Map<String, Map<String, String>> stateMap,
      List<String> partitions, List<InstanceSpec> instances, String field) {
    Set<String> validPartitions = new HashSet<>(partitions);
    Set<String> validInstances = new HashSet<>();
    for (InstanceSpec instance : instances) {
      validInstances.add(instance.name);
    }
    for (Map.Entry<String, Map<String, String>> partition : stateMap.entrySet()) {
      if (!validPartitions.contains(partition.getKey())) {
        throw invalid(field + " contains unknown partition: " + partition.getKey());
      }
      for (String instance : partition.getValue().keySet()) {
        if (!validInstances.contains(instance)) {
          throw invalid(field + " contains unknown instance: " + instance);
        }
      }
    }
  }

  private static List<String> requiredTextArray(JsonNode parent, String field) {
    JsonNode array = requiredArray(parent, field);
    List<String> values = new ArrayList<>();
    for (JsonNode value : array) {
      if (!value.isTextual()) {
        throw invalid(field + " must contain only strings");
      }
      values.add(value.textValue());
    }
    return values;
  }

  private static JsonNode requiredArray(JsonNode parent, String field) {
    JsonNode value = parent.get(field);
    if (value == null || !value.isArray()) {
      throw invalid("missing array field: " + field);
    }
    return value;
  }

  private static JsonNode requiredObject(JsonNode parent, String field) {
    JsonNode value = parent.get(field);
    if (value == null || !value.isObject()) {
      throw invalid("missing object field: " + field);
    }
    return value;
  }

  private static String requiredText(JsonNode parent, String field) {
    JsonNode value = parent.get(field);
    if (value == null || !value.isTextual() || value.textValue().isEmpty()) {
      throw invalid("missing string field: " + field);
    }
    return value.textValue();
  }

  private static int requiredInt(JsonNode parent, String field) {
    JsonNode value = parent.get(field);
    if (value == null || !value.isInt()) {
      throw invalid("missing integer field: " + field);
    }
    return value.intValue();
  }

  private static void requireObject(JsonNode value, String description) {
    if (value == null || !value.isObject()) {
      throw invalid(description + " must be an object");
    }
  }

  private static IllegalArgumentException invalid(String message) {
    return new IllegalArgumentException(message);
  }

  static final class InstanceSpec {
    final String name;
    final String sessionId;
    final Map<String, String> domain;

    InstanceSpec(String name, String sessionId, Map<String, String> domain) {
      this.name = name;
      this.sessionId = sessionId;
      this.domain = domain;
    }
  }

  static final class StateCount {
    final String state;
    final int count;

    StateCount(String state, int count) {
      this.state = state;
      this.count = count;
    }
  }

  static final class TopologySpec {
    final String path;
    final String faultZoneType;
    final String endNodeType;

    TopologySpec(String path, String faultZoneType, String endNodeType) {
      this.path = path;
      this.faultZoneType = faultZoneType;
      this.endNodeType = endNodeType;
    }
  }

  static final class StateQuery {
    final String from;
    final String to;

    StateQuery(String from, String to) {
      this.from = from;
      this.to = to;
    }
  }

  static final class TransitionSpec {
    final String partition;
    final String instance;
    final String from;
    final String to;

    TransitionSpec(String partition, String instance, String from, String to) {
      this.partition = partition;
      this.instance = instance;
      this.from = from;
      this.to = to;
    }
  }

  static final class M6ResourceSpec {
    final String name;
    final int replicas;
    final int minActiveReplicas;
    final Map<String, List<String>> preferenceLists;
    final Map<String, Map<String, String>> currentState;
    final Map<String, Map<String, String>> bestPossibleState;
    final List<M6TransitionSpec> selectedTransitions;

    M6ResourceSpec(String name, int replicas, int minActiveReplicas,
        Map<String, List<String>> preferenceLists,
        Map<String, Map<String, String>> currentState,
        Map<String, Map<String, String>> bestPossibleState,
        List<M6TransitionSpec> selectedTransitions) {
      this.name = name;
      this.replicas = replicas;
      this.minActiveReplicas = minActiveReplicas;
      this.preferenceLists = preferenceLists;
      this.currentState = currentState;
      this.bestPossibleState = bestPossibleState;
      this.selectedTransitions = selectedTransitions;
    }
  }

  static final class M6ThrottleConfig {
    final String rebalanceType;
    final String scope;
    final int maxTransitions;

    M6ThrottleConfig(String rebalanceType, String scope, int maxTransitions) {
      this.rebalanceType = rebalanceType;
      this.scope = scope;
      this.maxTransitions = maxTransitions;
    }
  }

  static final class M6TransitionSpec {
    final String resource;
    final String partition;
    final String instance;
    final String from;
    final String to;

    M6TransitionSpec(String resource, String partition, String instance, String from,
        String to) {
      this.resource = resource;
      this.partition = partition;
      this.instance = instance;
      this.from = from;
      this.to = to;
    }
  }

  static final class M7ResourceSpec {
    final String name;
    final Map<String, Map<String, String>> currentState;

    M7ResourceSpec(String name, Map<String, Map<String, String>> currentState) {
      this.name = name;
      this.currentState = currentState;
    }
  }

  static final class M7RoutingQuery {
    final String id;
    final String resource;
    final String partition;
    final String state;

    M7RoutingQuery(String id, String resource, String partition, String state) {
      this.id = id;
      this.resource = resource;
      this.partition = partition;
      this.state = state;
    }
  }
}
