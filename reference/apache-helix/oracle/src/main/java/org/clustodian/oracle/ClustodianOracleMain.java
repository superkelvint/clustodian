package org.clustodian.oracle;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class ClustodianOracleMain {
  private ClustodianOracleMain() {
  }

  public static void main(String[] args) {
    try {
      if (args.length != 1) {
        throw new IllegalArgumentException("usage: run-java-oracle.sh <scenario.json>");
      }
      PrintStream canonicalStdout = System.out;
      ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
      ObjectNode result;
      try (PrintStream capturedStdout = new PrintStream(diagnostics, true,
          StandardCharsets.UTF_8)) {
        System.setOut(capturedStdout);
        ScenarioV1 scenario = ScenarioV1.read(Path.of(args[0]));
        if (scenario.operation.equals("inspect_state_model")) {
          result = StateModelOracle.inspect(scenario);
        } else if (scenario.operation.equals("compute_semi_auto_best_possible")) {
          result = SemiAutoOracle.compute(scenario);
        } else if (scenario.operation.equals("compute_crush_assignment")) {
          result = CrushOracle.compute(scenario);
        } else if (scenario.operation.equals("select_transitions")) {
          result = MessageSelectionOracle.select(scenario);
        } else if (scenario.operation.equals("compute_intermediate_and_throttle")) {
          result = IntermediateThrottleOracle.compute(scenario);
        } else if (scenario.operation.equals("compute_external_view_and_routing")) {
          result = ExternalViewOracle.compute(scenario);
        } else {
          result = MessageGenerationOracle.generate(scenario);
        }
      } finally {
        System.setOut(canonicalStdout);
      }
      if (diagnostics.size() > 0) {
        System.err.print(new String(diagnostics.toByteArray(), StandardCharsets.UTF_8));
        System.err.flush();
      }
      System.out.println(ScenarioV1.MAPPER.writeValueAsString(result));
    } catch (Exception exception) {
      System.err.println("helix M0 oracle failed: " + exception.getMessage());
      exception.printStackTrace(System.err);
      System.exit(1);
    }
  }
}
