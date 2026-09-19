#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
APACHE_HELIX_SOURCE="${CLUSTODIAN_APACHE_HELIX_SOURCE:-$ROOT/reference/apache-helix-2.0.1}"
MAVEN_REPOSITORY="$ROOT/reference/apache-helix/.m2"
APACHE_HELIX_JAR="$APACHE_HELIX_SOURCE/helix-core/target/helix-core-2.0.1.jar"
LOCAL_APACHE_HELIX_JAR="$MAVEN_REPOSITORY/org/apache/helix/helix-core/2.0.1/helix-core-2.0.1.jar"
SOURCE_MARKER="$MAVEN_REPOSITORY/.helix-core-source-marker"
REPOSITORY_REVISION="$(git -C "$ROOT" rev-parse HEAD)"

[[ -f "$APACHE_HELIX_SOURCE/pom.xml" ]] || {
  echo "missing pinned Apache Helix source: $APACHE_HELIX_SOURCE/pom.xml" >&2
  exit 1
}

mkdir -p "$MAVEN_REPOSITORY"

if [[ -f "$APACHE_HELIX_JAR" && -f "$LOCAL_APACHE_HELIX_JAR" && -f "$SOURCE_MARKER" ]]; then
  if [[ "$(awk -F= '$1 == "repository_revision" {print $2}' "$SOURCE_MARKER")" == "$REPOSITORY_REVISION" ]] \
    && cmp -s "$APACHE_HELIX_JAR" "$LOCAL_APACHE_HELIX_JAR"; then
    echo "using cached pinned helix-core artifact for Git revision $REPOSITORY_REVISION" >&2
    exit 0
  fi
fi

mvn \
  -Dmaven.repo.local="$MAVEN_REPOSITORY" \
  -f "$APACHE_HELIX_SOURCE/pom.xml" \
  -pl helix-core \
  -am \
  install \
  -DskipTests

if [[ ! -f "$APACHE_HELIX_JAR" || ! -f "$LOCAL_APACHE_HELIX_JAR" ]]; then
  echo "Helix build did not produce the expected helix-core artifact" >&2
  exit 1
fi
if ! cmp -s "$APACHE_HELIX_JAR" "$LOCAL_APACHE_HELIX_JAR"; then
  echo "local helix-core artifact differs from the pinned source build" >&2
  exit 1
fi

temporary_marker="$(mktemp "${SOURCE_MARKER}.XXXXXX")"
trap 'rm -f "$temporary_marker"' EXIT
printf 'repository_revision=%s\n' "$REPOSITORY_REVISION" > "$temporary_marker"
mv -- "$temporary_marker" "$SOURCE_MARKER"
trap - EXIT
