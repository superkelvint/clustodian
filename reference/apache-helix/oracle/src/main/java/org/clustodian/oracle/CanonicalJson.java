package org.clustodian.oracle;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

final class CanonicalJson {
  static final String IMPLEMENTATION = "apache-helix";
  static final String APACHE_HELIX_VERSION = "2.0.1";
  static final String APACHE_HELIX_TAG = "helix-2.0.1";

  private CanonicalJson() {
  }

  static ObjectNode result(String operation) {
    ObjectNode result = ScenarioV1.MAPPER.createObjectNode();
    result.put("result_schema_version", 1);
    ObjectNode oracle = result.putObject("oracle");
    oracle.put("implementation", IMPLEMENTATION);
    oracle.put("helix_version", APACHE_HELIX_VERSION);
    oracle.put("helix_tag", APACHE_HELIX_TAG);
    oracle.put("commit_provenance", "tag = helix-2.0.1; commit metadata unavailable");
    result.put("operation", operation);
    return result;
  }

  static ArrayNode array(ObjectNode parent, String field) {
    return parent.putArray(field);
  }
}
