package org.clustodian.oracle;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;

final class SemiAutoOracle {
  private SemiAutoOracle() {
  }

  static ObjectNode compute(ScenarioV1 scenario) {
    StateModelDefinition stateModel = StateModelOracle.definition(scenario.stateModelName);
    IdealState idealState = new IdealState(scenario.resourceName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setStateModelDefRef(scenario.stateModelName);
    idealState.setNumPartitions(scenario.partitions.size());
    idealState.setReplicas(Integer.toString(scenario.replicas));
    for (String partition : scenario.partitions) {
      idealState.setPreferenceList(partition, scenario.preferenceLists.get(partition));
    }

    Resource resource = new Resource(scenario.resourceName);
    resource.setStateModelDefRef(scenario.stateModelName);
    for (String partition : scenario.partitions) {
      resource.addPartition(partition);
    }

    CurrentStateOutput currentState = new CurrentStateOutput();
    for (String partitionName : scenario.partitions) {
      Map<String, String> partitionState = scenario.currentState.getOrDefault(partitionName,
          Collections.emptyMap());
      for (Map.Entry<String, String> entry : partitionState.entrySet()) {
        currentState.setCurrentState(scenario.resourceName, new Partition(partitionName),
            entry.getKey(), entry.getValue());
      }
    }

    Map<String, LiveInstance> liveInstances = liveInstances(scenario);
    BaseControllerDataProvider cache = mock(BaseControllerDataProvider.class);
    when(cache.getStateModelDef(scenario.stateModelName)).thenReturn(stateModel);
    when(cache.getLiveInstances()).thenReturn(liveInstances);
    when(cache.getAssignableLiveInstances()).thenReturn(liveInstances);
    when(cache.getDisabledInstancesForPartition(anyString(), anyString()))
        .thenReturn(Collections.emptySet());
    when(cache.getClusterConfig()).thenReturn(new ClusterConfig("m3"));
    when(cache.getAbnormalStateResolver(anyString()))
        .thenReturn(MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER);

    ResourceAssignment assignment = new SemiAutoRebalancer<BaseControllerDataProvider>()
        .computeBestPossiblePartitionState(cache, idealState, resource, currentState);

    ObjectNode result = CanonicalJson.result(scenario.operation);
    ObjectNode bestPossibleState = result.putObject("best_possible_state");
    for (String partitionName : scenario.partitions) {
      ObjectNode partitionState = bestPossibleState.putObject(partitionName);
      Map<String, String> replicaMap = new TreeMap<>(
          assignment.getReplicaMap(new Partition(partitionName)));
      for (Map.Entry<String, String> entry : replicaMap.entrySet()) {
        partitionState.put(entry.getKey(), entry.getValue());
      }
    }
    return result;
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
