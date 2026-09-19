package org.clustodian.oracle;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.StateModelDefinition;

final class StateModelOracle {
  private StateModelOracle() {
  }

  static ObjectNode inspect(ScenarioV1 scenario) {
    StateModelDefinition definition = definition(scenario.stateModelName);
    ObjectNode result = CanonicalJson.result(scenario.operation);
    ObjectNode stateModel = result.putObject("state_model");
    stateModel.put("name", scenario.stateModelName);
    stateModel.put("initial_state", definition.getInitialState());
    stateModel.put("valid", definition.isValid());
    stateModel.put("top_state", definition.getTopState());
    stateModel.put("single_top_state", definition.isSingleTopStateModel());

    addStrings(stateModel, "states_priority", definition.getStatesPriorityList());
    addStrings(stateModel, "transition_priority", definition.getStateTransitionPriorityList());

    ObjectNode counts = stateModel.putObject("state_counts");
    for (String state : definition.getStatesPriorityList()) {
      String count = definition.getNumInstancesPerState(state);
      if (count == null) {
        counts.putNull(state);
      } else {
        counts.put(state, count);
      }
    }

    ArrayNode nextStates = stateModel.putArray("next_states");
    for (ScenarioV1.StateQuery query : scenario.stateQueries) {
      ObjectNode nextState = nextStates.addObject();
      nextState.put("from", query.from);
      nextState.put("to", query.to);
      String next = definition.getNextStateForTransition(query.from, query.to);
      if (next == null) {
        nextState.putNull("next");
      } else {
        nextState.put("next", next);
      }
    }
    return result;
  }

  static StateModelDefinition definition(String name) {
    if (!name.equals(BuiltInStateModelDefinitions.LeaderStandby.name())) {
      throw new IllegalArgumentException("unsupported built-in state model: " + name);
    }
    return BuiltInStateModelDefinitions.LeaderStandby.getStateModelDefinition();
  }

  private static void addStrings(ObjectNode parent, String field, List<String> values) {
    ArrayNode array = parent.putArray(field);
    for (String value : values) {
      array.add(value);
    }
  }
}
