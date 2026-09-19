#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
APACHE_HELIX_SOURCE="${CLUSTODIAN_APACHE_HELIX_SOURCE:-$ROOT/reference/apache-helix-2.0.1}"
MAVEN_REPOSITORY="$ROOT/reference/apache-helix/.m2"
ORACLE="$ROOT/reference/apache-helix/integration-oracle"
APACHE_HELIX_TEST_JAR="$MAVEN_REPOSITORY/org/apache/helix/helix-core/2.0.1/helix-core-2.0.1-tests.jar"

[[ -f "$APACHE_HELIX_SOURCE/pom.xml" ]] || {
  echo "missing pinned Apache Helix source: $APACHE_HELIX_SOURCE/pom.xml" >&2
  exit 1
}

command -v mvn >/dev/null 2>&1 || {
  echo "maven is required to build the M8 Java integration oracle" >&2
  exit 1
}

mkdir -p "$MAVEN_REPOSITORY"

# M0 owns source provenance. If the repository has an explicit source verifier,
# use it here as an additional guard. M8 must never silently compile against a
# Maven-Central helix-core in place of the pinned source tree.
if [[ -x "$ROOT/reference/apache-helix/scripts/verify-apache-helix-reference.sh" ]]; then
  "$ROOT/reference/apache-helix/scripts/verify-apache-helix-reference.sh" >&2
fi

# Build/install the pinned source into the isolated conformance Maven repo.
# -DskipTests intentionally still compiles test sources and allows helix-core's
# test-jar to be attached; the M8 oracle uses Helix's real integration helpers.
mvn \
  -q \
  -Dmaven.repo.local="$MAVEN_REPOSITORY" \
  -f "$APACHE_HELIX_SOURCE/pom.xml" \
  -pl helix-core \
  -am \
  install \
  -DskipTests \
  >&2

[[ -f "$APACHE_HELIX_TEST_JAR" ]] || {
  echo "pinned Helix build did not produce required test jar: $APACHE_HELIX_TEST_JAR" >&2
  echo "M8 requires the real Helix integration-test classes (TestHelper, ZkTestHelper, MockParticipantManager)." >&2
  exit 1
}

mvn \
  -q \
  -Dmaven.repo.local="$MAVEN_REPOSITORY" \
  -f "$ORACLE/pom.xml" \
  package \
  dependency:build-classpath \
  -Dmdep.outputFile="$ORACLE/target/classpath.txt" \
  -Dmdep.includeScope=runtime \
  -DskipTests \
  >&2
