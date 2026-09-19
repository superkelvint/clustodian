package org.clustodian.oracle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.examples.LeaderStandbyStateModelFactory;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.zkclient.ZkServer;

/**
 * Thin M8 integration adapter around real Apache Helix 2.0.1 + ZooKeeper.
 *
 * <p>The adapter drives real MockParticipantManager instances, forces real
 * ZooKeeper session expiration, and seeds test CurrentState records. Checkpoint
 * state is then obtained by running Helix's real ReadClusterDataStage,
 * ResourceComputationStage, and CurrentStateComputationStage. The adapter does not choose which session's
 * CurrentState is authoritative.</p>
 */
public final class ParticipantSessionIntegrationOracle {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  private static final long WAIT_TIMEOUT_MS = 20_000L;
  private static final long WAIT_STEP_MS = 25L;
  private static final long CHECKPOINT_MIN_SETTLE_MS = 500L;
  private static final int CHECKPOINT_STABLE_POLLS = 8;
  private static final long CHECKPOINT_POLL_MS = 50L;
  private static final String DEFAULT_STATE_MODEL = "LeaderStandby";
  private static final String DEFAULT_STATE_MODEL_FACTORY = "DEFAULT";

  private ParticipantSessionIntegrationOracle() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("usage: ParticipantSessionIntegrationOracle <scenario.json>");
      System.exit(2);
    }

    JsonNode scenario = MAPPER.readTree(Files.readString(Path.of(args[0])));

    // Helix/ZooKeeper test infrastructure writes lifecycle diagnostics to
    // stdout. Keep stdout reserved for exactly one machine-readable JSON value.
    PrintStream resultOut = System.out;
    System.setOut(System.err);
    ObjectNode result = runScenario(scenario);
    resultOut.println(MAPPER.writeValueAsString(result));
  }

  private static ObjectNode runScenario(JsonNode scenario) throws Exception {
    requireOperation(scenario);

    int port = TestHelper.getRandomPort();
    String zkAddress = "127.0.0.1:" + port;
    String cluster = "clustodian_m8_" + UUID.randomUUID().toString().replace("-", "");

    ZkServer zkServer = null;
    ClusterSetup clusterSetup = null;
    HelixManager observerManager = null;

    Map<String, MockParticipantManager> participants = new HashMap<>();
    Map<String, String> logicalToRawSession = new LinkedHashMap<>();
    Map<String, String> rawToLogicalSession = new HashMap<>();
    Map<String, String> resourceStateModels = parseResourceStateModels(scenario);
    Map<String, Resource> resources = buildResources(scenario);
    List<String> instances = requiredStringArray(scenario, "instances");
    ArrayNode checkpoints = MAPPER.createArrayNode();

    try {
      zkServer = TestHelper.startZkServer(zkAddress);

      // addCluster(..., true) installs Helix's built-in state-model definitions.
      clusterSetup = new ClusterSetup(zkAddress);
      clusterSetup.addCluster(cluster, true);

      int instancePort = 12000;
      for (String instance : instances) {
        InstanceConfig config = new InstanceConfig(instance);
        config.setHostName("127.0.0.1");
        config.setPort(Integer.toString(instancePort++));
        clusterSetup.getClusterManagementTool().addInstance(cluster, config);
      }

      // Install real Helix IdealState metadata for every resource declared by
      // the scenario. ResourceComputationStage will derive the Resource map
      // consumed by CurrentStateComputationStage from this ZooKeeper-backed
      // metadata, matching the normal controller pipeline. M8 does not use the
      // preference lists for placement semantics; they exist only to make the
      // declared partition set concrete to Helix.
      installScenarioResources(clusterSetup, cluster, scenario, instances);

      // A real spectator manager supplies the real accessor used by
      // ReadClusterDataStage. Spectators do not create participant LiveInstance
      // records, so this observer is outside the participant semantics under test.
      observerManager = HelixManagerFactory.getZKHelixManager(
          cluster,
          "m8-oracle-observer",
          InstanceType.SPECTATOR,
          zkAddress);
      observerManager.connect();

      HelixDataAccessor accessor = observerManager.getHelixDataAccessor();
      PropertyKey.Builder keys = accessor.keyBuilder();

      JsonNode steps = scenario.get("steps");
      if (steps == null || !steps.isArray()) {
        throw new IllegalArgumentException("scenario.steps must be an array");
      }

      for (JsonNode step : steps) {
        String op = requiredText(step, "op");
        switch (op) {
          case "connect":
            connect(
                zkAddress,
                cluster,
                accessor,
                keys,
                participants,
                logicalToRawSession,
                rawToLogicalSession,
                requiredText(step, "instance"),
                requiredText(step, "session"));
            break;

          case "disconnect":
            disconnect(
                accessor,
                keys,
                participants,
                logicalToRawSession,
                requiredText(step, "instance"),
                requiredText(step, "session"));
            break;

          case "expire_and_reconnect":
            expireAndReconnect(
                accessor,
                keys,
                participants,
                logicalToRawSession,
                rawToLogicalSession,
                requiredText(step, "instance"),
                requiredText(step, "from_session"),
                requiredText(step, "to_session"));
            break;

          case "publish_current_state":
            publishCurrentState(
                accessor,
                keys,
                resourceStateModels,
                logicalToRawSession,
                true,
                step);
            break;

          case "inject_session_current_state":
            publishCurrentState(
                accessor,
                keys,
                resourceStateModels,
                logicalToRawSession,
                false,
                step);
            break;

          case "checkpoint":
            checkpoints.add(stableCheckpoint(
                cluster,
                observerManager,
                resources,
                instances,
                rawToLogicalSession,
                requiredText(step, "id")));
            break;

          default:
            throw new IllegalArgumentException("unsupported M8 step op: " + op);
        }
      }

      ObjectNode result = MAPPER.createObjectNode();
      result.put("operation", "participant_session_semantics");
      result.set("checkpoints", checkpoints);
      result.set(
          "session_comparisons",
          buildSessionComparisons(scenario, logicalToRawSession));
      return result;
    } finally {
      List<MockParticipantManager> running = new ArrayList<>(participants.values());
      for (MockParticipantManager participant : running) {
        try {
          participant.syncStop();
        } catch (RuntimeException ignored) {
          // Best-effort cleanup after a failing oracle scenario.
        }
      }
      if (observerManager != null) {
        try {
          observerManager.disconnect();
        } catch (RuntimeException ignored) {
          // Best-effort cleanup.
        }
      }
      if (clusterSetup != null) {
        try {
          clusterSetup.close();
        } catch (RuntimeException ignored) {
          // Best-effort cleanup.
        }
      }
      if (zkServer != null) {
        TestHelper.stopZkServer(zkServer);
      }
    }
  }

  private static void connect(
      String zkAddress,
      String cluster,
      HelixDataAccessor accessor,
      PropertyKey.Builder keys,
      Map<String, MockParticipantManager> participants,
      Map<String, String> logicalToRawSession,
      Map<String, String> rawToLogicalSession,
      String instance,
      String logicalSession) throws Exception {
    if (participants.containsKey(instance)) {
      throw new IllegalStateException("participant already connected: " + instance);
    }

    MockParticipantManager participant =
        new MockParticipantManager(zkAddress, cluster, instance);

    // The supplied M8 corpus uses LeaderStandby. Register the real Helix
    // factory before connecting so participant new-session behavior has the
    // same state-model machinery as a normal embedded Helix participant.
    participant.getStateMachineEngine().registerStateModelFactory(
        DEFAULT_STATE_MODEL, new LeaderStandbyStateModelFactory());

    participant.syncStart();
    participants.put(instance, participant);

    String rawSession = waitForLiveSession(accessor, keys, instance, null);
    bindSession(logicalSession, rawSession, logicalToRawSession, rawToLogicalSession);
  }

  private static void disconnect(
      HelixDataAccessor accessor,
      PropertyKey.Builder keys,
      Map<String, MockParticipantManager> participants,
      Map<String, String> logicalToRawSession,
      String instance,
      String logicalSession) throws Exception {
    MockParticipantManager participant = participants.get(instance);
    if (participant == null) {
      throw new IllegalStateException("participant is not connected: " + instance);
    }

    String expectedRaw = requireBoundSession(logicalToRawSession, logicalSession);
    String currentRaw = currentLiveSession(accessor, keys, instance);
    if (!Objects.equals(expectedRaw, currentRaw)) {
      throw new IllegalStateException(
          "disconnect session mismatch for " + instance + ": expected "
              + logicalSession + " but live session differs");
    }

    participant.syncStop();
    participants.remove(instance);
    waitUntil("LiveInstance removal for " + instance,
        () -> currentLiveSession(accessor, keys, instance) == null);
  }

  private static void expireAndReconnect(
      HelixDataAccessor accessor,
      PropertyKey.Builder keys,
      Map<String, MockParticipantManager> participants,
      Map<String, String> logicalToRawSession,
      Map<String, String> rawToLogicalSession,
      String instance,
      String fromLogicalSession,
      String toLogicalSession) throws Exception {
    MockParticipantManager participant = participants.get(instance);
    if (participant == null) {
      throw new IllegalStateException("participant is not connected: " + instance);
    }

    String oldRaw = requireBoundSession(logicalToRawSession, fromLogicalSession);
    String currentRaw = currentLiveSession(accessor, keys, instance);
    if (!Objects.equals(oldRaw, currentRaw)) {
      throw new IllegalStateException(
          "expire session mismatch for " + instance + ": expected "
              + fromLogicalSession + " but live session differs");
    }

    // This is Helix's real integration-test helper. It forces the underlying
    // ZooKeeper session to expire; the participant then executes its normal
    // new-session path and publishes a replacement LiveInstance.
    ZkTestHelper.expireSession(participant.getZkClient());

    String newRaw = waitForLiveSession(accessor, keys, instance, oldRaw);
    if (Objects.equals(oldRaw, newRaw)) {
      throw new IllegalStateException("ZooKeeper session did not change for " + instance);
    }
    bindSession(toLogicalSession, newRaw, logicalToRawSession, rawToLogicalSession);
  }

  private static void publishCurrentState(
      HelixDataAccessor accessor,
      PropertyKey.Builder keys,
      Map<String, String> resourceStateModels,
      Map<String, String> logicalToRawSession,
      boolean requireActiveSession,
      JsonNode step) throws Exception {
    String instance = requiredText(step, "instance");
    String logicalSession = requiredText(step, "session");
    String resource = requiredText(step, "resource");
    String rawSession = requireBoundSession(logicalToRawSession, logicalSession);

    if (requireActiveSession) {
      String currentRaw = currentLiveSession(accessor, keys, instance);
      if (!Objects.equals(rawSession, currentRaw)) {
        throw new IllegalStateException(
            "publish_current_state requires the named session to be live for " + instance);
      }
    }

    PropertyKey key = keys.currentState(instance, rawSession, resource);
    CurrentState currentState = accessor.getProperty(key);
    if (currentState == null) {
      currentState = new CurrentState(resource);
    }
    currentState.setSessionId(rawSession);
    currentState.setStateModelDefRef(
        resourceStateModels.getOrDefault(resource, DEFAULT_STATE_MODEL));
    currentState.setStateModelFactoryName(DEFAULT_STATE_MODEL_FACTORY);

    JsonNode states = step.get("states");
    if (states == null || !states.isObject()) {
      throw new IllegalArgumentException("current-state step requires object field 'states'");
    }
    Iterator<Map.Entry<String, JsonNode>> stateEntries = states.fields();
    while (stateEntries.hasNext()) {
      Map.Entry<String, JsonNode> entry = stateEntries.next();
      if (!entry.getValue().isTextual() || entry.getValue().asText().isEmpty()) {
        throw new IllegalArgumentException("CurrentState values must be non-empty strings");
      }
      currentState.setState(entry.getKey(), entry.getValue().asText());
    }

    if (!accessor.setProperty(key, currentState)) {
      throw new IllegalStateException(
          "failed to write CurrentState for " + instance + "/" + logicalSession
              + "/" + resource);
    }
  }

  /**
   * Wait for the observable Helix snapshot to settle. Participant new-session
   * handling can update LiveInstance and CurrentState asynchronously; comparing
   * a transient intermediate snapshot would make the oracle flaky. This wait
   * does not define semantics: every sample is produced by the real Helix
   * controller stages below.
   */
  private static ObjectNode stableCheckpoint(
      String cluster,
      HelixManager observerManager,
      Map<String, Resource> resources,
      List<String> instances,
      Map<String, String> rawToLogicalSession,
      String checkpointId) throws Exception {
    long start = System.nanoTime();
    long deadline = start + WAIT_TIMEOUT_MS * 1_000_000L;
    long settleAfter = start + CHECKPOINT_MIN_SETTLE_MS * 1_000_000L;
    ObjectNode previous = null;
    int stablePolls = 0;

    while (System.nanoTime() < deadline) {
      ObjectNode current = computeCheckpoint(
          cluster, observerManager, resources, instances, rawToLogicalSession, checkpointId);

      if (current.equals(previous)) {
        stablePolls++;
      } else {
        previous = current;
        stablePolls = 1;
      }

      if (System.nanoTime() >= settleAfter && stablePolls >= CHECKPOINT_STABLE_POLLS) {
        return current;
      }
      Thread.sleep(CHECKPOINT_POLL_MS);
    }

    throw new IllegalStateException("timed out waiting for stable checkpoint " + checkpointId);
  }

  /**
   * Compute one semantic snapshot using Helix's real controller pipeline:
   * ReadClusterDataStage -> ResourceComputationStage ->
   * CurrentStateComputationStage. In particular, the adapter does not read
   * LiveInstance and then manually choose the matching CURRENTSTATES session
   * directory, and it does not hand-construct the Resource event attribute.
   */
  private static ObjectNode computeCheckpoint(
      String cluster,
      HelixManager observerManager,
      Map<String, Resource> resources,
      List<String> instances,
      Map<String, String> rawToLogicalSession,
      String checkpointId) throws Exception {
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
    ClusterEvent event = new ClusterEvent(
        cluster,
        ClusterEventType.OnDemandRebalance,
        "m8-checkpoint-" + checkpointId + "-" + UUID.randomUUID());
    event.addAttribute(AttributeName.helixmanager.name(), observerManager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Follow the real Helix controller stage dependency chain.
    // ReadClusterDataStage refreshes the controller cache from ZooKeeper;
    // ResourceComputationStage derives the Resource map required by
    // CurrentStateComputationStage from the real IdealState metadata installed
    // at scenario setup.
    new ReadClusterDataStage().process(event);
    new ResourceComputationStage().process(event);
    new CurrentStateComputationStage().process(event);

    CurrentStateOutput currentState = event.getAttribute(AttributeName.CURRENT_STATE.name());
    if (currentState == null) {
      throw new IllegalStateException(
          "CurrentStateComputationStage did not produce CurrentStateOutput");
    }

    ObjectNode output = MAPPER.createObjectNode();
    output.put("id", checkpointId);
    ObjectNode liveInstances = MAPPER.createObjectNode();
    ObjectNode activeCurrentState = MAPPER.createObjectNode();

    Map<String, LiveInstance> liveMap = cache.getLiveInstances();
    for (String instance : instances) {
      LiveInstance live = liveMap.get(instance);
      if (live == null) {
        continue;
      }

      String rawSession = live.getSessionId();
      String logicalSession = rawToLogicalSession.get(rawSession);
      if (logicalSession == null) {
        throw new IllegalStateException(
            "observed an unbound Helix session for " + instance + ": " + rawSession);
      }

      liveInstances.put(instance, logicalSession);

      ObjectNode active = MAPPER.createObjectNode();
      active.put("session", logicalSession);
      ObjectNode resourceStates = MAPPER.createObjectNode();

      for (Map.Entry<String, Resource> resourceEntry : new TreeMap<>(resources).entrySet()) {
        String resourceName = resourceEntry.getKey();
        ObjectNode partitionStates = MAPPER.createObjectNode();

        List<Partition> partitions = new ArrayList<>(resourceEntry.getValue().getPartitions());
        partitions.sort((left, right) -> left.getPartitionName().compareTo(right.getPartitionName()));
        for (Partition partition : partitions) {
          Map<String, String> stateMap = currentState.getCurrentStateMap(resourceName, partition);
          String state = stateMap.get(instance);
          if (state != null) {
            partitionStates.put(partition.getPartitionName(), state);
          }
        }

        if (!partitionStates.isEmpty()) {
          resourceStates.set(resourceName, partitionStates);
        }
      }

      active.set("resources", resourceStates);
      activeCurrentState.set(instance, active);
    }

    output.set("live_instances", liveInstances);
    output.set("active_current_state", activeCurrentState);
    return output;
  }

  private static ArrayNode buildSessionComparisons(
      JsonNode scenario,
      Map<String, String> logicalToRawSession) {
    ArrayNode output = MAPPER.createArrayNode();
    JsonNode comparisons = scenario.get("session_comparisons");
    if (comparisons == null) {
      return output;
    }
    if (!comparisons.isArray()) {
      throw new IllegalArgumentException("session_comparisons must be an array");
    }

    for (JsonNode comparison : comparisons) {
      String left = requiredText(comparison, "left");
      String right = requiredText(comparison, "right");
      String leftRaw = requireBoundSession(logicalToRawSession, left);
      String rightRaw = requireBoundSession(logicalToRawSession, right);

      ObjectNode result = MAPPER.createObjectNode();
      result.put("left", left);
      result.put("right", right);
      result.put("equal", Objects.equals(leftRaw, rightRaw));
      output.add(result);
    }
    return output;
  }

  private static String waitForLiveSession(
      HelixDataAccessor accessor,
      PropertyKey.Builder keys,
      String instance,
      String differentFrom) throws Exception {
    final String[] observed = new String[1];
    waitUntil("LiveInstance session for " + instance, () -> {
      String session = currentLiveSession(accessor, keys, instance);
      if (session == null || Objects.equals(session, differentFrom)) {
        return false;
      }
      observed[0] = session;
      return true;
    });
    return observed[0];
  }

  private static String currentLiveSession(
      HelixDataAccessor accessor,
      PropertyKey.Builder keys,
      String instance) {
    LiveInstance live = accessor.getProperty(keys.liveInstance(instance));
    return live == null ? null : live.getSessionId();
  }

  private static void waitUntil(String description, Supplier<Boolean> condition)
      throws Exception {
    long deadline = System.nanoTime() + WAIT_TIMEOUT_MS * 1_000_000L;
    while (System.nanoTime() < deadline) {
      if (condition.get()) {
        return;
      }
      Thread.sleep(WAIT_STEP_MS);
    }
    throw new IllegalStateException("timed out waiting for " + description);
  }

  private static void bindSession(
      String logical,
      String raw,
      Map<String, String> logicalToRaw,
      Map<String, String> rawToLogical) {
    if (logicalToRaw.containsKey(logical)) {
      throw new IllegalArgumentException("session label already bound: " + logical);
    }
    if (rawToLogical.containsKey(raw)) {
      throw new IllegalStateException(
          "raw session already bound to logical label " + rawToLogical.get(raw));
    }
    logicalToRaw.put(logical, raw);
    rawToLogical.put(raw, logical);
  }

  private static String requireBoundSession(Map<String, String> sessions, String logical) {
    String raw = sessions.get(logical);
    if (raw == null) {
      throw new IllegalArgumentException("unknown session label: " + logical);
    }
    return raw;
  }

  private static void installScenarioResources(
      ClusterSetup clusterSetup,
      String cluster,
      JsonNode scenario,
      List<String> instances) {
    JsonNode resourceSpecs = scenario.get("resources");
    if (resourceSpecs == null || !resourceSpecs.isArray()) {
      throw new IllegalArgumentException("scenario.resources must be an array");
    }

    for (JsonNode resourceSpec : resourceSpecs) {
      String resourceName = requiredText(resourceSpec, "name");
      String stateModel = resourceSpec.has("state_model")
          ? requiredText(resourceSpec, "state_model")
          : DEFAULT_STATE_MODEL;
      List<String> partitions = requiredStringArray(resourceSpec, "partitions");

      IdealState idealState = new IdealState(resourceName);
      idealState.setStateModelDefRef(stateModel);
      idealState.setNumPartitions(partitions.size());
      idealState.setReplicas(Integer.toString(Math.max(1, instances.size())));
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);

      for (String partition : partitions) {
        // Preference-list contents are not under test in M8. They simply make
        // the scenario's explicit partition names part of the real IdealState
        // from which ResourceComputationStage constructs Resource objects.
        idealState.setPreferenceList(partition, new ArrayList<>(instances));
      }

      clusterSetup.getClusterManagementTool().addResource(
          cluster, resourceName, idealState);
    }
  }

  private static Map<String, String> parseResourceStateModels(JsonNode scenario) {
    Map<String, String> result = new HashMap<>();
    JsonNode resources = scenario.get("resources");
    if (resources == null || !resources.isArray()) {
      throw new IllegalArgumentException("scenario.resources must be an array");
    }
    for (JsonNode resource : resources) {
      String name = requiredText(resource, "name");
      String stateModel = resource.has("state_model")
          ? requiredText(resource, "state_model")
          : DEFAULT_STATE_MODEL;
      if (result.put(name, stateModel) != null) {
        throw new IllegalArgumentException("duplicate resource: " + name);
      }
    }
    return result;
  }

  private static Map<String, Resource> buildResources(JsonNode scenario) {
    Map<String, Resource> result = new LinkedHashMap<>();
    JsonNode resources = scenario.get("resources");
    if (resources == null || !resources.isArray()) {
      throw new IllegalArgumentException("scenario.resources must be an array");
    }

    for (JsonNode resourceSpec : resources) {
      String name = requiredText(resourceSpec, "name");
      String stateModel = resourceSpec.has("state_model")
          ? requiredText(resourceSpec, "state_model")
          : DEFAULT_STATE_MODEL;
      Resource resource = new Resource(name);
      resource.setStateModelDefRef(stateModel);
      for (String partition : requiredStringArray(resourceSpec, "partitions")) {
        resource.addPartition(partition);
      }
      if (result.put(name, resource) != null) {
        throw new IllegalArgumentException("duplicate resource: " + name);
      }
    }
    return result;
  }

  private static List<String> requiredStringArray(JsonNode parent, String field) {
    JsonNode node = parent.get(field);
    if (node == null || !node.isArray()) {
      throw new IllegalArgumentException(field + " must be an array");
    }
    List<String> result = new ArrayList<>();
    for (JsonNode value : node) {
      if (!value.isTextual() || value.asText().isEmpty()) {
        throw new IllegalArgumentException(field + " values must be non-empty strings");
      }
      result.add(value.asText());
    }
    return result;
  }

  private static void requireOperation(JsonNode scenario) {
    String operation = requiredText(scenario, "operation");
    if (!"participant_session_semantics".equals(operation)) {
      throw new IllegalArgumentException("unsupported operation: " + operation);
    }
  }

  private static String requiredText(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || !value.isTextual() || value.asText().isEmpty()) {
      throw new IllegalArgumentException("missing/invalid text field: " + field);
    }
    return value.asText();
  }
}
