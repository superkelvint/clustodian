#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
ORACLE="$ROOT/reference/apache-helix/integration-oracle"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <scenario.json>" >&2
  exit 2
fi

if [[ ! -f "$ORACLE/target/classpath.txt" || \
      ! -f "$ORACLE/target/classes/org/clustodian/oracle/ParticipantRuntimeIntegrationOracle.class" ]]; then
  "$SCRIPT_DIR/build-java-integration-oracle.sh"
fi

CLASSPATH="$ORACLE/target/classes:$(<"$ORACLE/target/classpath.txt")"
exec java -cp "$CLASSPATH" \
  org.clustodian.oracle.ParticipantRuntimeIntegrationOracle "$1"
