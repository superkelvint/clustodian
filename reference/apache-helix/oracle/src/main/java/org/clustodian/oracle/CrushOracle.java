package org.clustodian.oracle;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class CrushOracle {
  private CrushOracle() {
  }

  static ObjectNode compute(ScenarioV1 scenario) {
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    for (ScenarioV1.StateCount stateCount : scenario.stateCounts) {
      if (stateCount.count < 0) {
        throw new IllegalArgumentException("state count must not be negative");
      }
      if (states.put(stateCount.state, stateCount.count) != null) {
        throw new IllegalArgumentException("duplicate state count: " + stateCount.state);
      }
    }

    Map<String, InstanceConfig> instanceConfigs = new LinkedHashMap<>();
    for (ScenarioV1.InstanceSpec instance : scenario.instances) {
      InstanceConfig config = new InstanceConfig(instance.name);
      config.setDomain(instance.domain);
      instanceConfigs.put(instance.name, config);
    }

    ClusterConfig clusterConfig = new ClusterConfig("m4-oracle-cluster");
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology(scenario.topology.path);
    clusterConfig.setFaultZoneType(scenario.topology.faultZoneType);

    ResourceControllerDataProvider dataProvider = mock(ResourceControllerDataProvider.class);
    when(dataProvider.getAssignableInstanceConfigMap()).thenReturn(instanceConfigs);
    when(dataProvider.getClusterConfig()).thenReturn(clusterConfig);
    when(dataProvider.getClusterEventId()).thenReturn("m4-oracle-event");

    CrushRebalanceStrategy strategy = new CrushRebalanceStrategy();
    strategy.init(scenario.resourceName, scenario.partitions, states,
        scenario.maxPartitionsPerInstance);
    List<String> liveNodes = scenario.liveInstances;
    ZNRecord record = strategy.computePartitionAssignment(
        scenario.instances.stream().map(instance -> instance.name).collect(Collectors.toList()),
        liveNodes,
        Map.of(),
        dataProvider);

    ObjectNode result = ScenarioV1.MAPPER.createObjectNode();
    result.put("implementation", "apache-helix");
    result.put("operation", "compute_crush_assignment");
    ObjectNode preferenceLists = result.putObject("preference_lists");
    for (String partition : scenario.partitions) {
      ArrayNode preferenceList = preferenceLists.putArray(partition);
      List<String> selected = record.getListField(partition);
      if (selected != null) {
        selected.forEach(preferenceList::add);
      }
    }
    return result;
  }
}
