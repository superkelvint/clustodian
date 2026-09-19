package org.clustodian.oracle;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.MessageGenerationPhase;
import org.apache.helix.controller.stages.MessageOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;

final class MessageGenerationOracle {
  private MessageGenerationOracle() {
  }

  static ObjectNode generate(ScenarioV1 scenario) throws Exception {
    StateModelDefinition stateModel = StateModelOracle.definition(scenario.stateModelName);
    Resource resource = new Resource(scenario.resourceName);
    resource.setStateModelDefRef(scenario.stateModelName);
    Map<String, Resource> resources = new LinkedHashMap<>();
    resources.put(scenario.resourceName, resource);

    CurrentStateOutput currentState = new CurrentStateOutput();
    BestPossibleStateOutput targetState = new BestPossibleStateOutput();
    for (String partitionName : scenario.partitions) {
      Partition partition = new Partition(partitionName);
      resource.addPartition(partitionName);
      Map<String, String> current = scenario.currentState.getOrDefault(partitionName,
          Collections.emptyMap());
      for (Map.Entry<String, String> entry : current.entrySet()) {
        currentState.setCurrentState(scenario.resourceName, partition, entry.getKey(),
            entry.getValue());
      }
      Map<String, String> target = scenario.targetState.getOrDefault(partitionName,
          Collections.emptyMap());
      for (Map.Entry<String, String> entry : target.entrySet()) {
        targetState.setState(scenario.resourceName, partition, entry.getKey(), entry.getValue());
      }
    }

    BaseControllerDataProvider cache = mock(BaseControllerDataProvider.class);
    when(cache.getStateModelDef(scenario.stateModelName)).thenReturn(stateModel);
    when(cache.getLiveInstances()).thenReturn(liveInstances(scenario));
    when(cache.getClusterConfig()).thenReturn(new ClusterConfig("m0"));
    when(cache.getStaleMessagesByInstance(anyString())).thenReturn(Collections.emptySet());
    when(cache.getAllInstances()).thenReturn(instanceNames(scenario));

    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    HelixManager manager = mock(HelixManager.class);
    when(manager.getInstanceName()).thenReturn("m0-controller");
    when(manager.getSessionId()).thenReturn("m0-controller-session");
    when(manager.getHelixDataAccessor()).thenReturn(accessor);

    ClusterEvent event = new ClusterEvent("m0", ClusterEventType.Unknown, "m0-oracle-event");
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resources);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentState);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), targetState);

    ByteArrayOutputStream helixStdout = new ByteArrayOutputStream();
    PrintStream oracleStdout = System.out;
    try (PrintStream capturedStdout = new PrintStream(
        helixStdout, true, StandardCharsets.UTF_8)) {
      System.setOut(capturedStdout);
      new MessageGenerationPhase().process(event);
    } finally {
      System.setOut(oracleStdout);
      if (helixStdout.size() > 0) {
        System.err.print(new String(helixStdout.toByteArray(), StandardCharsets.UTF_8));
        System.err.flush();
      }
    }
    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    if (output == null) {
      throw new IllegalStateException("MessageGenerationPhase did not produce MessageOutput");
    }

    List<Transition> transitions = new ArrayList<>();
    for (String partitionName : scenario.partitions) {
      Partition partition = new Partition(partitionName);
      for (Message message : output.getMessages(scenario.resourceName, partition)) {
        transitions.add(new Transition(message.getResourceName(), message.getPartitionName(),
            message.getTgtName(), message.getFromState(), message.getToState(),
            message.getMsgType()));
      }
    }
    transitions.sort(Comparator.comparing(Transition::sortKey));

    ObjectNode result = CanonicalJson.result(scenario.operation);
    ArrayNode transitionArray = result.putArray("transitions");
    for (Transition transition : transitions) {
      ObjectNode value = transitionArray.addObject();
      value.put("resource", transition.resource);
      value.put("partition", transition.partition);
      value.put("instance", transition.instance);
      value.put("from", transition.from);
      value.put("to", transition.to);
      value.put("message_type", transition.messageType);
    }
    return result;
  }

  private static Map<String, LiveInstance> liveInstances(ScenarioV1 scenario) {
    Map<String, LiveInstance> liveInstances = new LinkedHashMap<>();
    for (ScenarioV1.InstanceSpec instance : scenario.instances) {
      LiveInstance liveInstance = new LiveInstance(instance.name);
      liveInstance.setSessionId(instance.sessionId);
      liveInstances.put(instance.name, liveInstance);
    }
    return liveInstances;
  }

  private static Set<String> instanceNames(ScenarioV1 scenario) {
    Set<String> names = new TreeSet<>();
    for (ScenarioV1.InstanceSpec instance : scenario.instances) {
      names.add(instance.name);
    }
    return names;
  }

  private static final class Transition {
    private final String resource;
    private final String partition;
    private final String instance;
    private final String from;
    private final String to;
    private final String messageType;

    private Transition(String resource, String partition, String instance, String from, String to,
        String messageType) {
      this.resource = resource;
      this.partition = partition;
      this.instance = instance;
      this.from = from;
      this.to = to;
      this.messageType = messageType;
    }

    private String sortKey() {
      return String.join("\u0000", resource, partition, instance, from, to, messageType);
    }
  }
}
