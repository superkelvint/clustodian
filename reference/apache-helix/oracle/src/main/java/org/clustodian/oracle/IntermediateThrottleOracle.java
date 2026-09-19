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
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.IntermediateStateOutput;
import org.apache.helix.controller.stages.MessageOutput;
import org.apache.helix.controller.stages.MessageThrottleStage;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;

final class IntermediateThrottleOracle {
  private IntermediateThrottleOracle() {
  }

  static ObjectNode compute(ScenarioV1 scenario) throws Exception {
    StateModelDefinition stateModel = StateModelOracle.definition(scenario.stateModelName);
    Map<String, Resource> resources = new LinkedHashMap<>();
    Map<String, IdealState> idealStates = new LinkedHashMap<>();
    CurrentStateOutput currentState = new CurrentStateOutput();
    BestPossibleStateOutput bestPossibleState = new BestPossibleStateOutput();
    MessageOutput selectedMessages = new MessageOutput();

    for (ScenarioV1.M6ResourceSpec spec : scenario.m6Resources) {
      Resource resource = new Resource(spec.name);
      resource.setStateModelDefRef(scenario.stateModelName);
      IdealState idealState = new IdealState(spec.name);
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setStateModelDefRef(scenario.stateModelName);
      idealState.setNumPartitions(spec.preferenceLists.size());
      idealState.setReplicas(Integer.toString(spec.replicas));
      idealState.setMinActiveReplicas(spec.minActiveReplicas);
      idealState.setPreferenceLists(spec.preferenceLists);

      for (String partitionName : spec.preferenceLists.keySet()) {
        resource.addPartition(partitionName);
        Partition partition = new Partition(partitionName);
        for (Map.Entry<String, String> entry : spec.currentState
            .getOrDefault(partitionName, Collections.emptyMap()).entrySet()) {
          currentState.setCurrentState(spec.name, partition, entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : spec.bestPossibleState
            .getOrDefault(partitionName, Collections.emptyMap()).entrySet()) {
          bestPossibleState.setState(spec.name, partition, entry.getKey(), entry.getValue());
        }
      }
      bestPossibleState.setPreferenceLists(spec.name, spec.preferenceLists);

      for (ScenarioV1.M6TransitionSpec transition : spec.selectedTransitions) {
        selectedMessages.addMessage(spec.name, new Partition(transition.partition),
            message(scenario, transition, "selected"));
      }
      resources.put(spec.name, resource);
      idealStates.put(spec.name, idealState);
    }

    for (ScenarioV1.M6TransitionSpec transition : scenario.m6PendingTransitions) {
      currentState.setPendingMessage(transition.resource, new Partition(transition.partition),
          transition.instance, message(scenario, transition, "pending"));
    }

    ClusterConfig clusterConfig = new ClusterConfig("m6");
    List<StateTransitionThrottleConfig> throttleConfigs = scenario.m6ThrottleConfigs.stream()
        .map(IntermediateThrottleOracle::throttleConfig)
        .collect(java.util.stream.Collectors.toList());
    clusterConfig.setStateTransitionThrottleConfigs(throttleConfigs);

    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(cache.getLiveInstances()).thenReturn(liveInstances(scenario));
    when(cache.getAssignableLiveInstances()).thenReturn(liveInstances(scenario));
    when(cache.getEnabledLiveInstances()).thenReturn(liveInstances(scenario).keySet());
    when(cache.getDisabledInstancesForPartition(anyString(), anyString()))
        .thenReturn(Collections.emptySet());
    for (Map.Entry<String, IdealState> entry : idealStates.entrySet()) {
      when(cache.getIdealState(entry.getKey())).thenReturn(entry.getValue());
    }
    when(cache.getStateModelDef(scenario.stateModelName)).thenReturn(stateModel);

    ClusterEvent event = new ClusterEvent("m6", ClusterEventType.Unknown, "m6-oracle-event");
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.RESOURCES.name(), resources);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resources);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentState);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleState);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), selectedMessages);

    new IntermediateStateCalcStage().process(event);
    new MessageThrottleStage().process(event);

    IntermediateStateOutput intermediate =
        event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());
    MessageOutput dispatchable = event.getAttribute(AttributeName.MESSAGES_THROTTLE.name());
    if (intermediate == null || dispatchable == null) {
      throw new IllegalStateException("M6 stages did not produce both outputs");
    }

    ObjectNode result = CanonicalJson.result(scenario.operation);
    ObjectNode intermediateJson = result.putObject("intermediate_state");
    for (String resourceName : new TreeSet<>(resources.keySet())) {
      ObjectNode resourceJson = intermediateJson.putObject(resourceName);
      Map<Partition, Map<String, String>> stateMap = intermediate
          .getPartitionStateMap(resourceName).getStateMap();
      List<Map.Entry<Partition, Map<String, String>>> partitions =
          new java.util.ArrayList<>(stateMap.entrySet());
      partitions.sort(java.util.Comparator.comparing(entry -> entry.getKey().getPartitionName()));
      for (Map.Entry<Partition, Map<String, String>> partition : partitions) {
        ObjectNode partitionJson = resourceJson.putObject(partition.getKey().getPartitionName());
        for (Map.Entry<String, String> instance : new TreeMap<>(partition.getValue()).entrySet()) {
          partitionJson.put(instance.getKey(), instance.getValue());
        }
      }
    }

    ArrayNode dispatchableJson = result.putArray("dispatchable_transitions");
    for (String resourceName : new TreeSet<>(resources.keySet())) {
      for (Partition partition : resources.get(resourceName).getPartitions()) {
        String partitionName = partition.getPartitionName();
        for (Message message : dispatchable.getMessages(resourceName, partition)) {
          ObjectNode transition = dispatchableJson.addObject();
          transition.put("resource", message.getResourceName());
          transition.put("partition", message.getPartitionName());
          transition.put("instance", message.getTgtName());
          transition.put("from", message.getFromState());
          transition.put("to", message.getToState());
          transition.put("message_type", message.getMsgType());
        }
      }
    }
    return result;
  }

  private static StateTransitionThrottleConfig throttleConfig(ScenarioV1.M6ThrottleConfig config) {
    return new StateTransitionThrottleConfig(
        StateTransitionThrottleConfig.RebalanceType.valueOf(config.rebalanceType),
        StateTransitionThrottleConfig.ThrottleScope.valueOf(config.scope),
        config.maxTransitions);
  }

  private static Message message(ScenarioV1 scenario, ScenarioV1.M6TransitionSpec transition,
      String kind) {
    Message message = new Message(MessageType.STATE_TRANSITION,
        "m6-" + kind + "-" + transition.resource + "-" + transition.partition + "-"
            + transition.instance + "-" + transition.from + "-" + transition.to);
    message.setSrcName("m6-controller");
    message.setTgtName(transition.instance);
    message.setMsgState(MessageState.NEW);
    message.setResourceName(transition.resource);
    message.setPartitionName(transition.partition);
    message.setFromState(transition.from);
    message.setToState(transition.to);
    message.setTgtSessionId("m6-session-" + transition.instance);
    message.setSrcSessionId("m6-controller-session");
    message.setStateModelDef(scenario.stateModelName);
    message.setStateModelFactoryName("DEFAULT");
    message.setBucketSize(0);
    return message;
  }

  private static Map<String, LiveInstance> liveInstances(ScenarioV1 scenario) {
    Set<String> liveNames = new TreeSet<>(scenario.m6LiveInstances);
    Map<String, LiveInstance> result = new LinkedHashMap<>();
    for (String name : liveNames) {
      LiveInstance live = new LiveInstance(name);
      live.setSessionId("m6-session-" + name);
      result.put(name, live);
    }
    return result;
  }
}
