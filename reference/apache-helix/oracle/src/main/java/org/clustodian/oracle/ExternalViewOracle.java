package org.clustodian.oracle;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.ExternalViewComputeStage;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;

final class ExternalViewOracle {
  private ExternalViewOracle() {
  }

  static ObjectNode compute(ScenarioV1 scenario) throws Exception {
    List<ExternalView> externalViews = computeExternalViews(scenario);
    ObjectNode result = CanonicalJson.result(scenario.operation);
    serializeExternalViews(result, externalViews);
    serializeRoutingResults(result, scenario, externalViews);
    return result;
  }

  private static List<ExternalView> computeExternalViews(ScenarioV1 scenario) throws Exception {
    Map<String, Resource> resources = new LinkedHashMap<>();
    CurrentStateOutput currentState = new CurrentStateOutput();
    for (ScenarioV1.M7ResourceSpec resourceSpec : scenario.m7Resources) {
      Resource resource = new Resource(resourceSpec.name);
      resources.put(resourceSpec.name, resource);
      for (Map.Entry<String, Map<String, String>> partitionEntry : resourceSpec.currentState
          .entrySet()) {
        Partition partition = new Partition(partitionEntry.getKey());
        resource.addPartition(partitionEntry.getKey());
        for (Map.Entry<String, String> instanceEntry : partitionEntry.getValue().entrySet()) {
          currentState.setCurrentState(resourceSpec.name, partition, instanceEntry.getKey(),
              instanceEntry.getValue());
        }
      }
    }

    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getExternalViews()).thenReturn(new LinkedHashMap<>());
    List<ExternalView> externalViews = new ArrayList<>();
    doAnswer(invocation -> {
      @SuppressWarnings("unchecked")
      List<ExternalView> views = (List<ExternalView>) invocation.getArgument(0);
      externalViews.addAll(views);
      return null;
    }).when(cache).updateExternalViews(anyList());

    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    when(accessor.keyBuilder()).thenReturn(keyBuilder);
    when(keyBuilder.externalView(anyString())).thenReturn(mock(PropertyKey.class));
    HelixManager manager = mock(HelixManager.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);

    ClusterEvent event = new ClusterEvent("m7", ClusterEventType.Unknown, "m7-oracle-event");
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resources);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentState);
    new ExternalViewComputeStage().execute(event);

    if (externalViews.size() != resources.size()) {
      throw new IllegalStateException("ExternalViewComputeStage did not publish every resource");
    }
    return externalViews;
  }

  private static void serializeExternalViews(ObjectNode result, List<ExternalView> externalViews) {
    ObjectNode externalView = result.putObject("external_view");
    Map<String, ExternalView> byResource = new TreeMap<>();
    for (ExternalView view : externalViews) {
      byResource.put(view.getResourceName(), view);
    }
    for (Map.Entry<String, ExternalView> resourceEntry : byResource.entrySet()) {
      ObjectNode partitions = externalView.putObject(resourceEntry.getKey());
      ExternalView view = resourceEntry.getValue();
      for (String partitionName : new TreeSet<>(view.getPartitionSet())) {
        ObjectNode instances = partitions.putObject(partitionName);
        for (Map.Entry<String, String> instanceEntry :
            new TreeMap<>(view.getStateMap(partitionName)).entrySet()) {
          instances.put(instanceEntry.getKey(), instanceEntry.getValue());
        }
      }
    }
  }

  private static void serializeRoutingResults(ObjectNode result, ScenarioV1 scenario,
      List<ExternalView> externalViews) throws Exception {
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    PropertyKey instanceConfigsKey = mock(PropertyKey.class);
    PropertyKey liveInstancesKey = mock(PropertyKey.class);
    when(keyBuilder.instanceConfigs()).thenReturn(instanceConfigsKey);
    when(keyBuilder.liveInstances()).thenReturn(liveInstancesKey);
    when(accessor.keyBuilder()).thenReturn(keyBuilder);

    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    for (String instanceName : scenario.m7Instances) {
      instanceConfigs.add(new InstanceConfig(instanceName));
    }
    doReturn(instanceConfigs).when(accessor).getChildValues(instanceConfigsKey, true);
    doReturn(Collections.<LiveInstance>emptyList()).when(accessor)
        .getChildValues(liveInstancesKey, true);
    HelixManager manager = mock(HelixManager.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);

    RoutingTableProvider provider =
        new RoutingTableProvider(null, PropertyType.EXTERNALVIEW, false, 0);
    try {
      provider.onExternalViewChange(externalViews, new NotificationContext(manager));
      RoutingTableSnapshot snapshot = provider.getRoutingTableSnapshot(PropertyType.EXTERNALVIEW);
      ArrayNode routingResults = result.putArray("routing_results");
      for (ScenarioV1.M7RoutingQuery query : scenario.m7RoutingQueries) {
        List<InstanceConfig> matched = snapshot.getInstancesForResource(query.resource,
            query.partition, query.state);
        List<String> instanceNames = new ArrayList<>();
        for (InstanceConfig config : matched) {
          instanceNames.add(config.getInstanceName());
        }
        Collections.sort(instanceNames);
        ObjectNode routingResult = routingResults.addObject();
        routingResult.put("id", query.id);
        routingResult.putPOJO("instances", instanceNames);
      }
    } finally {
      provider.shutdown();
    }
  }
}
