package org.clustodian.oracle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType;
import org.apache.helix.api.config.StateTransitionThrottleConfig.ThrottleScope;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.ClusterManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.zookeeper.zkclient.ZkServer;

/** Real Helix controller oracle for M10 scenario checkpoints. */
public final class ControllerRuntimeIntegrationOracle {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  private static final long WAIT_TIMEOUT_MS = 20_000L;
  private static final long POLL_MS = 50L;

  /** Participant liveness/session owner with no state-model executor. */
  private static final class PassiveParticipantManager extends ClusterManager {
    private PassiveParticipantManager(String zkAddress, String cluster, String instance) {
      super(zkAddress, cluster, instance, InstanceType.PARTICIPANT);
    }
  }

  private ControllerRuntimeIntegrationOracle() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: ControllerRuntimeIntegrationOracle <scenario.json>");
    }
    JsonNode scenario = MAPPER.readTree(java.nio.file.Files.readString(java.nio.file.Path.of(args[0])));
    run(scenario, System.out, System.err);
  }

  public static void run(JsonNode scenario, PrintStream resultOut, PrintStream logOut)
      throws Exception {
    PrintStream originalOut = System.out;
    System.setOut(logOut);
    ObjectNode result;
    try {
      result = runScenario(scenario);
    } finally {
      System.setOut(originalOut);
    }
    resultOut.println(MAPPER.writeValueAsString(result));
  }

  private static ObjectNode runScenario(JsonNode scenario) throws Exception {
    int port = TestHelper.getRandomPort();
    String zkAddress = "127.0.0.1:" + port;
    String cluster = "clustodian_m10_" + UUID.randomUUID().toString().replace("-", "");
    ZkServer zkServer = null;
    ClusterSetup setup = null;
    ClusterControllerManager controller = null;
    HelixManager observer = null;
    Map<String, PassiveParticipantManager> participants = new HashMap<>();
    Map<String, String> logicalToRaw = new LinkedHashMap<>();
    Map<String, String> rawToLogical = new HashMap<>();
    Map<String, List<String>> resourcePartitions = new TreeMap<>();
    ArrayNode checkpoints = MAPPER.createArrayNode();

    try {
      zkServer = TestHelper.startZkServer(zkAddress);
      setup = new ClusterSetup(zkAddress);
      setup.addCluster(cluster, true);

      List<String> instances = installInstanceConfigs(setup, cluster, scenario);
      observer = HelixManagerFactory.getZKHelixManager(
          cluster, "m10-oracle-observer", InstanceType.SPECTATOR, zkAddress);
      observer.connect();
      for (JsonNode operation : requiredArray(scenario, "setup")) {
        applyOperation(operation, setup, cluster, zkAddress, instances, participants,
            logicalToRaw, rawToLogical, resourcePartitions, observer);
      }

      controller = new ClusterControllerManager(zkAddress, cluster, "m10-controller");
      controller.syncStart();

      for (JsonNode operation : requiredArray(scenario, "steps")) {
        if ("checkpoint".equals(requiredText(operation, "op"))) {
          checkpoints.add(stableCheckpoint(observer, participants, resourcePartitions,
              instances, rawToLogical, requiredText(operation, "id")));
        } else {
          applyOperation(operation, setup, cluster, zkAddress, instances, participants,
              logicalToRaw, rawToLogical, resourcePartitions, observer);
        }
      }

      ObjectNode result = MAPPER.createObjectNode();
      result.put("operation", "controller_runtime_semantics");
      result.set("checkpoints", checkpoints);
      return result;
    } finally {
      if (controller != null) {
        try {
          controller.syncStop();
        } catch (RuntimeException ignored) {
          // Best effort cleanup.
        }
      }
      for (PassiveParticipantManager participant : participants.values()) {
        try {
          participant.syncStop();
        } catch (RuntimeException ignored) {
          // Best effort cleanup.
        }
      }
      if (observer != null) {
        try {
          observer.disconnect();
        } catch (RuntimeException ignored) {
          // Best effort cleanup.
        }
      }
      if (setup != null) {
        try {
          setup.close();
        } catch (RuntimeException ignored) {
          // Best effort cleanup.
        }
      }
      if (zkServer != null) {
        TestHelper.stopZkServer(zkServer);
      }
    }
  }

  private static List<String> installInstanceConfigs(
      ClusterSetup setup, String cluster, JsonNode scenario) {
    List<String> instances = new ArrayList<>();
    for (JsonNode instanceNode : requiredArray(scenario, "instance_configs")) {
      String name = requiredText(instanceNode, "name");
      String zone = requiredText(instanceNode, "zone");
      InstanceConfig config = new InstanceConfig(name);
      config.setHostName("127.0.0.1");
      config.setPort(Integer.toString(12000 + instances.size()));
      config.setDomain("zone=" + zone + ",instance=" + name);
      setup.getClusterManagementTool().addInstance(cluster, config);
      instances.add(name);
    }
    return instances;
  }

  private static void applyOperation(
      JsonNode operation,
      ClusterSetup setup,
      String cluster,
      String zkAddress,
      List<String> instances,
      Map<String, PassiveParticipantManager> participants,
      Map<String, String> logicalToRaw,
      Map<String, String> rawToLogical,
      Map<String, List<String>> resourcePartitions,
      HelixManager observer) throws Exception {
    String op = requiredText(operation, "op");
    switch (op) {
      case "put_resource":
        putResource(setup, cluster, operation.get("resource"), instances, resourcePartitions);
        break;
      case "connect":
        connect(zkAddress, cluster, operation, participants, logicalToRaw, rawToLogical);
        break;
      case "disconnect":
        disconnect(operation, participants, logicalToRaw);
        break;
      case "expire_and_reconnect":
        expireAndReconnect(operation, participants, logicalToRaw, rawToLogical);
        break;
      case "publish_current_state":
        publishCurrentState(observer, resourcePartitions, logicalToRaw, operation);
        break;
      case "set_transition_throttle":
        setThrottle(observer, operation);
        break;
      case "clear_transition_throttles":
        clearThrottles(observer);
        break;
      default:
        throw new IllegalArgumentException("unsupported M10 operation: " + op);
    }
  }

  private static void putResource(
      ClusterSetup setup,
      String cluster,
      JsonNode definition,
      List<String> instances,
      Map<String, List<String>> resourcePartitions) {
    String name = requiredText(definition, "name");
    String stateModel = requiredText(definition, "state_model");
    JsonNode placement = definition.get("placement");
    String kind = requiredText(placement, "kind");
    int replicas = placement.get("replicas").asInt();
    List<String> partitions = new ArrayList<>();
    if ("CRUSH".equals(kind)) {
      for (JsonNode partition : requiredArray(placement, "partitions")) {
        partitions.add(partition.asText());
      }
    } else {
      Iterator<Map.Entry<String, JsonNode>> fields = requiredObject(placement, "preference_lists").fields();
      while (fields.hasNext()) {
        partitions.add(fields.next().getKey());
      }
    }
    resourcePartitions.put(name, partitions);

    IdealState idealState = new IdealState(name);
    idealState.setStateModelDefRef(stateModel);
    idealState.setNumPartitions(partitions.size());
    idealState.setReplicas(Integer.toString(replicas));
    if ("CRUSH".equals(kind)) {
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setRebalanceStrategy(CrushRebalanceStrategy.class.getName());
      for (String partition : partitions) {
        idealState.setPreferenceList(partition, new ArrayList<>(instances));
      }
    } else {
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      JsonNode preferences = requiredObject(placement, "preference_lists");
      for (String partition : partitions) {
        List<String> preference = new ArrayList<>();
        for (JsonNode instance : requiredArray(preferences, partition)) {
          preference.add(instance.asText());
        }
        idealState.setPreferenceList(partition, preference);
      }
    }
    if (setup.getClusterManagementTool().getResourceIdealState(cluster, name) == null) {
      setup.getClusterManagementTool().addResource(cluster, name, idealState);
    } else {
      setup.getClusterManagementTool().setResourceIdealState(cluster, name, idealState);
    }
  }

  private static void connect(
      String zkAddress,
      String cluster,
      JsonNode operation,
      Map<String, PassiveParticipantManager> participants,
      Map<String, String> logicalToRaw,
      Map<String, String> rawToLogical) throws Exception {
    String instance = requiredText(operation, "instance");
    String logical = requiredText(operation, "session");
    PassiveParticipantManager participant = new PassiveParticipantManager(zkAddress, cluster, instance);
    participant.syncStart();
    participants.put(instance, participant);
    String raw = waitForLiveSession(participant, logicalToRaw.values().stream()
        .filter(Objects::nonNull).findFirst().orElse(null));
    bind(logical, raw, logicalToRaw, rawToLogical);
  }

  private static void disconnect(
      JsonNode operation,
      Map<String, PassiveParticipantManager> participants,
      Map<String, String> logicalToRaw) throws Exception {
    String instance = requiredText(operation, "instance");
    String expected = requireSession(logicalToRaw, requiredText(operation, "session"));
    PassiveParticipantManager participant = participants.remove(instance);
    if (participant == null || !expected.equals(participant.getSessionId())) {
      throw new IllegalStateException("disconnect session mismatch for " + instance);
    }
    participant.syncStop();
  }

  private static void expireAndReconnect(
      JsonNode operation,
      Map<String, PassiveParticipantManager> participants,
      Map<String, String> logicalToRaw,
      Map<String, String> rawToLogical) throws Exception {
    String instance = requiredText(operation, "instance");
    String oldLogical = requiredText(operation, "from_session");
    String newLogical = requiredText(operation, "to_session");
    PassiveParticipantManager participant = participants.get(instance);
    String oldRaw = requireSession(logicalToRaw, oldLogical);
    if (participant == null || !oldRaw.equals(participant.getSessionId())) {
      throw new IllegalStateException("expire session mismatch for " + instance);
    }
    ZkTestHelper.expireSession(participant.getZkClient());
    String newRaw = waitForLiveSession(participant, oldRaw);
    bind(newLogical, newRaw, logicalToRaw, rawToLogical);
  }

  private static void publishCurrentState(
      HelixManager observer,
      Map<String, List<String>> resourcePartitions,
      Map<String, String> logicalToRaw,
      JsonNode operation) {
    String instance = requiredText(operation, "instance");
    String session = requireSession(logicalToRaw, requiredText(operation, "session"));
    String resource = requiredText(operation, "resource");
    HelixDataAccessor accessor = observer.getHelixDataAccessor();
    CurrentState current = accessor.getProperty(
        accessor.keyBuilder().currentState(instance, session, resource));
    if (current == null) {
      current = new CurrentState(resource);
    }
    current.setSessionId(session);
    current.setStateModelDefRef("LeaderStandby");
    current.setStateModelFactoryName("DEFAULT");
    Iterator<Map.Entry<String, JsonNode>> fields = requiredObject(operation, "states").fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      current.setState(field.getKey(), field.getValue().asText());
    }
    if (!accessor.setProperty(accessor.keyBuilder().currentState(instance, session, resource), current)) {
      throw new IllegalStateException("failed to publish CurrentState");
    }
    if (!resourcePartitions.containsKey(resource)) {
      throw new IllegalStateException("unknown resource " + resource);
    }
  }

  private static void setThrottle(HelixManager observer, JsonNode operation) {
    ClusterConfig config = observer.getHelixDataAccessor().getProperty(
        observer.getHelixDataAccessor().keyBuilder().clusterConfig());
    config.setStateTransitionThrottleConfigs(ImmutableList.of(new StateTransitionThrottleConfig(
        RebalanceType.valueOf(requiredText(operation, "rebalance_type")),
        ThrottleScope.valueOf(requiredText(operation, "scope")),
        operation.get("max_in_flight").asLong())));
    observer.getHelixDataAccessor().setProperty(
        observer.getHelixDataAccessor().keyBuilder().clusterConfig(), config);
  }

  private static void clearThrottles(HelixManager observer) {
    HelixDataAccessor accessor = observer.getHelixDataAccessor();
    ClusterConfig config = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    config.setStateTransitionThrottleConfigs(ImmutableList.of());
    accessor.setProperty(accessor.keyBuilder().clusterConfig(), config);
  }

  private static ObjectNode stableCheckpoint(
      HelixManager observer,
      Map<String, PassiveParticipantManager> participants,
      Map<String, List<String>> resourcePartitions,
      List<String> instances,
      Map<String, String> rawToLogical,
      String id) throws Exception {
    ObjectNode previous = null;
    int stable = 0;
    long deadline = System.nanoTime() + WAIT_TIMEOUT_MS * 1_000_000L;
    while (System.nanoTime() < deadline) {
      ObjectNode current = checkpoint(observer, participants, resourcePartitions, instances,
          rawToLogical, id);
      if (current.equals(previous)) {
        stable++;
      } else {
        previous = current;
        stable = 1;
      }
      if (stable >= 8) {
        return current;
      }
      Thread.sleep(POLL_MS);
    }
    throw new IllegalStateException("timed out waiting for checkpoint " + id);
  }

  private static ObjectNode checkpoint(
      HelixManager observer,
      Map<String, PassiveParticipantManager> participants,
      Map<String, List<String>> resourcePartitions,
      List<String> instances,
      Map<String, String> rawToLogical,
      String id) {
    HelixDataAccessor accessor = observer.getHelixDataAccessor();
    PropertyKey.Builder keys = accessor.keyBuilder();
    ObjectNode output = MAPPER.createObjectNode();
    output.put("id", id);
    ObjectNode liveOutput = MAPPER.createObjectNode();
    ObjectNode activeOutput = MAPPER.createObjectNode();
    for (String instance : instances) {
      LiveInstance live = accessor.getProperty(keys.liveInstance(instance));
      if (live == null) {
        continue;
      }
      String raw = live.getSessionId();
      String logical = rawToLogical.get(raw);
      if (logical == null) {
        throw new IllegalStateException("unbound live session " + raw);
      }
      liveOutput.put(instance, logical);
      ObjectNode resources = MAPPER.createObjectNode();
      for (Map.Entry<String, List<String>> resource : resourcePartitions.entrySet()) {
        CurrentState current = accessor.getProperty(
            keys.currentState(instance, raw, resource.getKey()));
        if (current == null) {
          continue;
        }
        ObjectNode partitions = MAPPER.createObjectNode();
        for (String partition : resource.getValue()) {
          String state = current.getState(partition);
          if (state != null) {
            partitions.put(partition, state);
          }
        }
        if (partitions.size() > 0) {
          resources.set(resource.getKey(), partitions);
        }
      }
      ObjectNode active = MAPPER.createObjectNode();
      active.put("session", logical);
      active.set("resources", resources);
      activeOutput.set(instance, active);
    }
    output.set("live_instances", liveOutput);
    output.set("active_current_state", activeOutput);

    ObjectNode external = MAPPER.createObjectNode();
    for (String resource : resourcePartitions.keySet()) {
      ExternalView view = accessor.getProperty(keys.externalView(resource));
      if (view == null) {
        continue;
      }
      ObjectNode partitions = MAPPER.createObjectNode();
      for (String partition : resourcePartitions.get(resource)) {
        Map<String, String> states = view.getStateMap(partition);
        if (states == null) {
          continue;
        }
        ObjectNode replicas = MAPPER.createObjectNode();
        for (Map.Entry<String, String> state : new TreeMap<>(states).entrySet()) {
          replicas.put(state.getKey(), state.getValue());
        }
        if (replicas.size() > 0) {
          partitions.set(partition, replicas);
        }
      }
      if (partitions.size() > 0) {
        external.set(resource, partitions);
      }
    }
    output.set("external_view", external);

    ArrayNode messages = MAPPER.createArrayNode();
    Set<String> seen = new HashSet<>();
    for (String instance : instances) {
      for (Message message : accessor.<Message>getChildValues(keys.messages(instance))) {
        String targetSession = rawToLogical.get(message.getTgtSessionId());
        if (targetSession == null || !"STATE_TRANSITION".equals(message.getMsgType())) {
          continue;
        }
        String key = String.join("\u0000", message.getResourceName(), message.getPartitionName(),
            instance, targetSession, message.getFromState(), message.getToState(), message.getMsgType());
        if (seen.add(key)) {
          messages.add(jsonMessage(message, instance, targetSession));
        }
      }
    }
    output.set("pending_transitions", messages);
    return output;
  }

  private static ObjectNode jsonMessage(Message message, String instance, String session) {
    ObjectNode value = MAPPER.createObjectNode();
    value.put("resource", message.getResourceName());
    value.put("partition", message.getPartitionName());
    value.put("instance", instance);
    value.put("target_session", session);
    value.put("from", message.getFromState());
    value.put("to", message.getToState());
    value.put("message_type", message.getMsgType());
    return value;
  }

  private static String waitForLiveSession(PassiveParticipantManager participant, String differentFrom)
      throws Exception {
    final String[] observed = new String[1];
    waitUntil(() -> {
      String session = participant.getSessionId();
      if (session == null || Objects.equals(session, differentFrom)) {
        return false;
      }
      observed[0] = session;
      return true;
    });
    return observed[0];
  }

  private static void waitUntil(Supplier<Boolean> condition) throws Exception {
    long deadline = System.nanoTime() + WAIT_TIMEOUT_MS * 1_000_000L;
    while (System.nanoTime() < deadline) {
      if (condition.get()) {
        return;
      }
      Thread.sleep(POLL_MS);
    }
    throw new IllegalStateException("timed out waiting for Helix condition");
  }

  private static void bind(String logical, String raw, Map<String, String> logicalToRaw,
      Map<String, String> rawToLogical) {
    if (logicalToRaw.put(logical, raw) != null || rawToLogical.put(raw, logical) != null) {
      throw new IllegalStateException("duplicate session binding " + logical);
    }
  }

  private static String requireSession(Map<String, String> sessions, String logical) {
    String raw = sessions.get(logical);
    if (raw == null) {
      throw new IllegalArgumentException("unknown session " + logical);
    }
    return raw;
  }

  private static JsonNode[] requiredArray(JsonNode parent, String field) {
    JsonNode node = parent.get(field);
    if (node == null || !node.isArray()) {
      throw new IllegalArgumentException(field + " must be an array");
    }
    JsonNode[] result = new JsonNode[node.size()];
    for (int i = 0; i < node.size(); i++) {
      result[i] = node.get(i);
    }
    return result;
  }

  private static JsonNode requiredObject(JsonNode parent, String field) {
    JsonNode node = parent.get(field);
    if (node == null || !node.isObject()) {
      throw new IllegalArgumentException(field + " must be an object");
    }
    return node;
  }

  private static String requiredText(JsonNode parent, String field) {
    JsonNode value = parent.get(field);
    if (value == null || !value.isTextual() || value.asText().isEmpty()) {
      throw new IllegalArgumentException("missing/invalid text field " + field);
    }
    return value.asText();
  }
}
