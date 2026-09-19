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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.zkclient.ZkServer;

/** Independent M11 oracle using the real Helix participant task machinery. */
public final class ParticipantRuntimeIntegrationOracle {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  private static final long WAIT_TIMEOUT_MS = 20_000L;
  private static final long WAIT_STEP_MS = 25L;
  private static final String MODEL = "LeaderStandby";
  private static final String FACTORY = "DEFAULT";

  private ParticipantRuntimeIntegrationOracle() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("usage: ParticipantRuntimeIntegrationOracle <scenario.json>");
      System.exit(2);
    }
    JsonNode scenario = MAPPER.readTree(Files.readString(Path.of(args[0])));
    PrintStream resultOut = System.out;
    System.setOut(System.err);
    resultOut.println(MAPPER.writeValueAsString(run(scenario)));
  }

  private static ObjectNode run(JsonNode scenario) throws Exception {
    require(scenario, "operation", "participant_runtime_semantics");
    JsonNode participantSpec = scenario.get("participant");
    String instance = text(participantSpec, "instance");
    String initialSession = text(participantSpec, "initial_session");
    if (!MODEL.equals(text(participantSpec, "state_model"))) {
      throw new IllegalArgumentException("M11 oracle supports LeaderStandby only");
    }
    Map<String, List<String>> resources = resources(scenario);
    int port = TestHelper.getRandomPort();
    String zkAddress = "127.0.0.1:" + port;
    String cluster = "clustodian_m11_" + UUID.randomUUID().toString().replace("-", "");
    ZkServer zkServer = null;
    ClusterSetup setup = null;
    HelixManager observer = null;
    MockParticipantManager participant = null;
    Recorder recorder = new Recorder(scenario.get("handler_behaviors"));
    Map<String, String> logicalToRaw = new LinkedHashMap<>();
    Map<String, String> rawToLogical = new HashMap<>();
    ArrayNode checkpoints = MAPPER.createArrayNode();
    try {
      zkServer = TestHelper.startZkServer(zkAddress);
      setup = new ClusterSetup(zkAddress);
      setup.addCluster(cluster, true);
      org.apache.helix.model.InstanceConfig config = new org.apache.helix.model.InstanceConfig(instance);
      config.setHostName("127.0.0.1");
      config.setPort("12000");
      setup.getClusterManagementTool().addInstance(cluster, config);
      installResources(setup, cluster, resources, instance);
      observer = org.apache.helix.HelixManagerFactory.getZKHelixManager(
          cluster, "m11-observer", InstanceType.SPECTATOR, zkAddress);
      observer.connect();
      HelixDataAccessor accessor = observer.getHelixDataAccessor();
      PropertyKey.Builder keys = accessor.keyBuilder();

      participant = new MockParticipantManager(zkAddress, cluster, instance);
      participant.getStateMachineEngine().registerStateModelFactory(MODEL, new RecordingFactory(recorder));
      participant.syncStart();
      String raw = waitForLive(accessor, keys, instance, null);
      bind(logicalToRaw, rawToLogical, initialSession, raw);

      for (JsonNode step : scenario.get("steps")) {
        String op = text(step, "op");
        switch (op) {
          case "send_transition":
            inject(accessor, keys, instance, logicalToRaw, step);
            break;
          case "wait_handler_entered":
            recorder.waitEntered(text(step, "message_id"), text(step, "token"));
            break;
          case "release_handler":
            recorder.release(text(step, "message_id"), text(step, "token"));
            break;
          case "expire_and_reconnect":
            String old = requireSession(logicalToRaw, text(step, "from_session"));
            ZkTestHelper.expireSession(participant.getZkClient());
            String replacement = waitForLive(accessor, keys, instance, old);
            bind(logicalToRaw, rawToLogical, text(step, "to_session"), replacement);
            break;
          case "checkpoint":
            checkpoints.add(stableCheckpoint(accessor, keys, instance, resources, rawToLogical, recorder,
                text(step, "id")));
            break;
          default:
            throw new IllegalArgumentException("unsupported M11 operation: " + op);
        }
      }
      ObjectNode result = MAPPER.createObjectNode();
      result.put("operation", "participant_runtime_semantics");
      result.set("checkpoints", checkpoints);
      return result;
    } finally {
      if (participant != null) {
        try { participant.syncStop(); } catch (RuntimeException ignored) { }
      }
      if (observer != null) {
        try { observer.disconnect(); } catch (RuntimeException ignored) { }
      }
      if (setup != null) {
        try { setup.close(); } catch (RuntimeException ignored) { }
      }
      if (zkServer != null) TestHelper.stopZkServer(zkServer);
    }
  }

  private static void inject(HelixDataAccessor accessor, PropertyKey.Builder keys, String instance,
      Map<String, String> sessions, JsonNode step) {
    String id = text(step, "message_id");
    Message message = new Message(Message.MessageType.STATE_TRANSITION, id);
    message.setSrcName("m11-controller");
    message.setTgtName(instance);
    message.setTgtSessionId(sessions.getOrDefault(text(step, "target_session"), "stale-session"));
    message.setResourceName(text(step, "resource"));
    message.setPartitionName(text(step, "partition"));
    message.setStateModelDef(MODEL);
    message.setStateModelFactoryName(FACTORY);
    message.setFromState(text(step, "from"));
    message.setToState(text(step, "to"));
    if (!accessor.setProperty(keys.message(instance, id), message)) {
      throw new IllegalStateException("failed to inject Helix message " + id);
    }
  }

  private static ObjectNode stableCheckpoint(HelixDataAccessor accessor, PropertyKey.Builder keys,
      String instance, Map<String, List<String>> resources, Map<String, String> rawToLogical,
      Recorder recorder, String id) throws Exception {
    ObjectNode previous = null;
    int stable = 0;
    long deadline = System.nanoTime() + WAIT_TIMEOUT_MS * 1_000_000L;
    while (System.nanoTime() < deadline) {
      ObjectNode current = checkpoint(accessor, keys, instance, resources, rawToLogical, recorder, id);
      if (current.equals(previous)) stable++; else { previous = current; stable = 1; }
      if (stable >= 8) return current;
      Thread.sleep(WAIT_STEP_MS);
    }
    throw new IllegalStateException("timed out waiting for stable checkpoint " + id);
  }

  private static ObjectNode checkpoint(HelixDataAccessor accessor, PropertyKey.Builder keys,
      String instance, Map<String, List<String>> resources, Map<String, String> rawToLogical,
      Recorder recorder, String id) {
    ObjectNode output = MAPPER.createObjectNode();
    output.put("id", id);
    ObjectNode liveInstances = MAPPER.createObjectNode();
    ObjectNode active = MAPPER.createObjectNode();
    LiveInstance live = accessor.getProperty(keys.liveInstance(instance));
    if (live != null) {
      String raw = live.getSessionId();
      String logical = rawToLogical.get(raw);
      if (logical == null) throw new IllegalStateException("unbound Helix session " + raw);
      liveInstances.put(instance, logical);
      ObjectNode activeEntry = MAPPER.createObjectNode();
      activeEntry.put("session", logical);
      ObjectNode resourceOutput = MAPPER.createObjectNode();
      for (Map.Entry<String, List<String>> resource : resources.entrySet()) {
        CurrentState current = accessor.getProperty(keys.currentState(instance, raw, resource.getKey()));
        ObjectNode partitions = MAPPER.createObjectNode();
        if (current != null) {
          for (String partition : resource.getValue()) {
            String state = current.getState(partition);
            if (state != null && !state.isEmpty()) partitions.put(partition, state);
          }
        }
        if (partitions.size() > 0) resourceOutput.set(resource.getKey(), partitions);
      }
      activeEntry.set("resources", resourceOutput);
      active.set(instance, activeEntry);
    }
    output.set("live_instances", liveInstances);
    output.set("active_current_state", active);
    ArrayNode pending = MAPPER.createArrayNode();
    List<String> messageIds = new ArrayList<>(accessor.getChildNames(keys.messages(instance)));
    Collections.sort(messageIds);
    for (String messageId : messageIds) {
      Message message = accessor.getProperty(keys.message(instance, messageId));
      if (message == null) continue;
      ObjectNode entry = MAPPER.createObjectNode();
      entry.put("message_id", message.getMsgId());
      entry.put("resource", message.getResourceName());
      entry.put("partition", message.getPartitionName());
      entry.put("target_session", rawToLogical.getOrDefault(message.getTgtSessionId(), "unknown"));
      entry.put("from", message.getFromState());
      entry.put("to", message.getToState());
      entry.put("message_type", message.getMsgType().toString());
      pending.add(entry);
    }
    output.set("pending_messages", pending);
    output.set("handler_events", recorder.events());
    return output;
  }

  private static void installResources(ClusterSetup setup, String cluster,
      Map<String, List<String>> resources, String instance) {
    for (Map.Entry<String, List<String>> resource : resources.entrySet()) {
      IdealState ideal = new IdealState(resource.getKey());
      ideal.setStateModelDefRef(MODEL);
      ideal.setNumPartitions(resource.getValue().size());
      ideal.setReplicas("1");
      ideal.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      for (String partition : resource.getValue()) ideal.setPreferenceList(partition, Collections.singletonList(instance));
      setup.getClusterManagementTool().addResource(cluster, resource.getKey(), ideal);
    }
  }

  private static Map<String, List<String>> resources(JsonNode scenario) {
    Map<String, List<String>> resources = new LinkedHashMap<>();
    for (JsonNode step : scenario.get("steps")) {
      if (!"send_transition".equals(text(step, "op"))) continue;
      String resource = text(step, "resource");
      resources.computeIfAbsent(resource, ignored -> new ArrayList<>());
      if (!resources.get(resource).contains(text(step, "partition"))) resources.get(resource).add(text(step, "partition"));
    }
    return resources;
  }

  private static String waitForLive(HelixDataAccessor accessor, PropertyKey.Builder keys,
      String instance, String differentFrom) throws Exception {
    final String[] result = new String[1];
    waitUntil("LiveInstance", () -> {
      LiveInstance live = accessor.getProperty(keys.liveInstance(instance));
      if (live == null || Objects.equals(differentFrom, live.getSessionId())) return false;
      result[0] = live.getSessionId();
      return true;
    });
    return result[0];
  }

  private static void waitUntil(String description, Supplier<Boolean> condition) throws Exception {
    long deadline = System.nanoTime() + WAIT_TIMEOUT_MS * 1_000_000L;
    while (System.nanoTime() < deadline) {
      if (condition.get()) return;
      Thread.sleep(WAIT_STEP_MS);
    }
    throw new IllegalStateException("timed out waiting for " + description);
  }

  private static void bind(Map<String, String> logicalToRaw, Map<String, String> rawToLogical,
      String logical, String raw) {
    logicalToRaw.put(logical, raw);
    rawToLogical.put(raw, logical);
  }

  private static String requireSession(Map<String, String> sessions, String logical) {
    String raw = sessions.get(logical);
    if (raw == null) throw new IllegalArgumentException("unknown session label " + logical);
    return raw;
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node == null ? null : node.get(field);
    if (value == null || !value.isTextual() || value.asText().isEmpty()) {
      throw new IllegalArgumentException("missing/invalid text field " + field);
    }
    return value.asText();
  }

  private static void require(JsonNode node, String field, String expected) {
    if (!expected.equals(text(node, field))) throw new IllegalArgumentException("unexpected " + field);
  }

  public static final class RecordingFactory extends StateModelFactory<RecordingModel> {
    private final Recorder recorder;
    RecordingFactory(Recorder recorder) { this.recorder = recorder; }
    @Override public RecordingModel createNewStateModel(String resource, String partition) {
      return new RecordingModel(recorder);
    }
  }

  public static final class RecordingModel extends StateModel {
    private final Recorder recorder;
    RecordingModel(Recorder recorder) { this.recorder = recorder; }
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) throws Exception { recorder.invoke(message); }
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) throws Exception { recorder.invoke(message); }
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) throws Exception { recorder.invoke(message); }
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) throws Exception { recorder.invoke(message); }
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) throws Exception { recorder.invoke(message); }
  }

  private static final class Recorder {
    private final Map<String, String> kinds = new HashMap<>();
    private final Map<String, String> tokens = new HashMap<>();
    private final Map<String, CountDownLatch> entered = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> released = new ConcurrentHashMap<>();
    private final List<ObjectNode> completed = new CopyOnWriteArrayList<>();
    Recorder(JsonNode behaviorNode) {
      if (behaviorNode != null && behaviorNode.isObject()) {
        Iterator<Map.Entry<String, JsonNode>> fields = behaviorNode.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> field = fields.next();
          kinds.put(field.getKey(), text(field.getValue(), "kind"));
          if (field.getValue().has("token")) tokens.put(field.getKey(), text(field.getValue(), "token"));
        }
      }
    }
    void invoke(Message message) throws Exception {
      String id = message.getMsgId();
      String kind = kinds.getOrDefault(id, "success");
      if ("block_then_success".equals(kind)) {
        String token = tokens.get(id);
        String key = id + ":" + token;
        CountDownLatch enteredLatch = entered.computeIfAbsent(key, ignored -> new CountDownLatch(1));
        released.computeIfAbsent(key, ignored -> new CountDownLatch(1));
        enteredLatch.countDown();
        long deadline = System.nanoTime() + WAIT_TIMEOUT_MS * 1_000_000L;
        boolean interrupted = false;
        while (released.get(key).getCount() != 0 && System.nanoTime() < deadline) {
          try {
            released.get(key).await(WAIT_STEP_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
          } catch (InterruptedException exception) {
            // Helix may interrupt application work while resetting its task
            // executor. The test handler deliberately completes after release
            // so the oracle exercises the session-fenced post-handler path.
            interrupted = true;
          }
        }
        if (interrupted) Thread.currentThread().interrupt();
      }
      ObjectNode event = MAPPER.createObjectNode();
      event.put("message_id", id);
      event.put("resource", message.getResourceName());
      event.put("partition", message.getPartitionName());
      event.put("from", message.getFromState());
      event.put("to", message.getToState());
      event.put("outcome", "error".equals(kind) ? "error" : "success");
      completed.add(event);
      if ("error".equals(kind)) throw new Exception("application transition failed");
    }
    void waitEntered(String id, String token) throws Exception {
      if (!entered.computeIfAbsent(id + ":" + token, ignored -> new CountDownLatch(1))
          .await(WAIT_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException("timed out waiting for callback entry");
      }
    }
    void release(String id, String token) {
      released.computeIfAbsent(id + ":" + token, ignored -> new CountDownLatch(1)).countDown();
    }
    ArrayNode events() {
      ArrayNode array = MAPPER.createArrayNode();
      for (ObjectNode event : completed) array.add(event.deepCopy());
      return array;
    }
  }
}
