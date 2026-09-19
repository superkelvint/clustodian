#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
MAVEN_REPOSITORY="$ROOT/reference/apache-helix/.m2"
ORACLE="$ROOT/reference/apache-helix/oracle"

"$SCRIPT_DIR/build-apache-helix-reference.sh" >&2

mkdir -p "$MAVEN_REPOSITORY"
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
