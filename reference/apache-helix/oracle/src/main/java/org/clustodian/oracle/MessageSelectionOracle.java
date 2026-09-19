package org.clustodian.oracle;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.MessageOutput;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;

final class MessageSelectionOracle {
  private MessageSelectionOracle() {
  }

  static ObjectNode select(ScenarioV1 scenario) throws Exception {
    StateModelDefinition stateModel = StateModelOracle.definition(scenario.stateModelName);
    Resource resource = new Resource(scenario.resourceName);
    resource.setStateModelDefRef(scenario.stateModelName);
    for (String partition : scenario.partitions) {
      resource.addPartition(partition);
    }
    Map<String, Resource> resources = new LinkedHashMap<>();
    resources.put(scenario.resourceName, resource);

    IdealState idealState = new IdealState(scenario.resourceName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setStateModelDefRef(scenario.stateModelName);
    idealState.setNumPartitions(scenario.partitions.size());
    idealState.setReplicas(Integer.toString(scenario.replicas));
    for (String partition : scenario.partitions) {
      idealState.setPreferenceList(partition, scenario.preferenceLists.get(partition));
    }

    CurrentStateOutput currentState = new CurrentStateOutput();
    for (String partitionName : scenario.partitions) {
      Partition partition = new Partition(partitionName);
      for (Map.Entry<String, String> entry : scenario.currentState
          .getOrDefault(partitionName, Collections.emptyMap()).entrySet()) {
        currentState.setCurrentState(scenario.resourceName, partition, entry.getKey(),
            entry.getValue());
      }
      for (ScenarioV1.TransitionSpec transition : scenario.pendingTransitions) {
        if (transition.partition.equals(partitionName)) {
          currentState.setPendingMessage(scenario.resourceName, partition, transition.instance,
              message(scenario, transition, "pending"));
        }
      }
    }

    MessageOutput candidates = new MessageOutput();
    for (ScenarioV1.TransitionSpec transition : scenario.candidateTransitions) {
      candidates.addMessage(scenario.resourceName, new Partition(transition.partition),
          message(scenario, transition, "candidate"));
    }

    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getStateModelDef(scenario.stateModelName)).thenReturn(stateModel);
    when(cache.getIdealState(scenario.resourceName)).thenReturn(idealState);
    when(cache.getLiveInstances()).thenReturn(liveInstances(scenario));
    when(cache.getClusterConfig()).thenReturn(new ClusterConfig("m5"));
    when(cache.getStaleMessagesByInstance(anyString())).thenReturn(Collections.emptySet());

    ClusterEvent event = new ClusterEvent("m5", ClusterEventType.Unknown, "m5-oracle-event");
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.RESOURCES.name(), resources);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentState);
    event.addAttribute(AttributeName.MESSAGES_ALL.name(), candidates);
    new MessageSelectionStage().process(event);

    MessageOutput selected = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    if (selected == null) {
      throw new IllegalStateException("MessageSelectionStage did not produce MessageOutput");
    }
    ObjectNode result = CanonicalJson.result(scenario.operation);
    ArrayNode selectedTransitions = result.putArray("selected_transitions");
    Map<String, List<Message>> byPartition = new TreeMap<>();
    for (String partitionName : scenario.partitions) {
      byPartition.put(partitionName,
          selected.getMessages(scenario.resourceName, new Partition(partitionName)));
    }
    for (List<Message> messages : byPartition.values()) {
      for (Message message : messages) {
        ObjectNode transition = selectedTransitions.addObject();
        transition.put("resource", message.getResourceName());
        transition.put("partition", message.getPartitionName());
        transition.put("instance", message.getTgtName());
        transition.put("from", message.getFromState());
        transition.put("to", message.getToState());
        transition.put("message_type", message.getMsgType());
      }
    }
    return result;
  }

  private static Message message(ScenarioV1 scenario, ScenarioV1.TransitionSpec transition,
      String kind) {
    Message message = new Message(MessageType.STATE_TRANSITION,
        "m5-" + kind + "-" + transition.partition + "-" + transition.instance + "-"
            + transition.from + "-" + transition.to);
    message.setSrcName("m5-controller");
    message.setTgtName(transition.instance);
    message.setMsgState(MessageState.NEW);
    message.setResourceName(scenario.resourceName);
    message.setPartitionName(transition.partition);
    message.setFromState(transition.from);
    message.setToState(transition.to);
    message.setTgtSessionId(sessionFor(scenario, transition.instance));
    message.setSrcSessionId("m5-controller-session");
    message.setStateModelDef(scenario.stateModelName);
    message.setStateModelFactoryName("DEFAULT");
    message.setBucketSize(0);
    return message;
  }

  private static String sessionFor(ScenarioV1 scenario, String instanceName) {
    for (ScenarioV1.InstanceSpec instance : scenario.instances) {
      if (instance.name.equals(instanceName)) {
        return instance.sessionId;
      }
    }
    throw new IllegalArgumentException("unknown instance: " + instanceName);
  }

  private static Map<String, LiveInstance> liveInstances(ScenarioV1 scenario) {
    Set<String> liveNames = new TreeSet<>(scenario.liveInstances);
    Map<String, LiveInstance> liveInstances = new LinkedHashMap<>();
    for (ScenarioV1.InstanceSpec instance : scenario.instances) {
      if (liveNames.contains(instance.name)) {
        LiveInstance liveInstance = new LiveInstance(instance.name);
        liveInstance.setSessionId(instance.sessionId);
        liveInstances.put(instance.name, liveInstance);
      }
    }
    return liveInstances;
  }
}
