#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
ORACLE="$ROOT/reference/apache-helix/integration-oracle"
MAVEN_REPOSITORY="$ROOT/reference/apache-helix/.m2"
MARKER="$ORACLE/target/.m10-build-marker"
REPOSITORY_REVISION="$(git -C "$ROOT" rev-parse HEAD)"

required_classes=(
  "$ORACLE/target/classes/org/clustodian/oracle/ControllerRuntimeIntegrationOracle.class"
)

if [[ -f "$MARKER" && -f "$ORACLE/target/classpath.txt" ]]; then
  if [[ "$(awk -F= '$1 == "repository_revision" {print $2}' "$MARKER")" == "$REPOSITORY_REVISION" ]]; then
    all_present=true
    for class_file in "${required_classes[@]}"; do
      if [[ ! -f "$class_file" ]]; then
        all_present=false
        break
      fi
    done
    if [[ "$all_present" == true ]]; then
      echo "using cached M10 Java integration oracle for Git revision $REPOSITORY_REVISION" >&2
      exit 0
    fi
  fi
fi

"$SCRIPT_DIR/build-java-integration-oracle.sh"

for class_file in "${required_classes[@]}"; do
  [[ -f "$class_file" ]] || {
    echo "M10 Java build did not produce required class: $class_file" >&2
    exit 1
  }
done

temporary_marker="$(mktemp "${MARKER}.XXXXXX")"
trap 'rm -f "$temporary_marker"' EXIT
printf 'repository_revision=%s\n' "$REPOSITORY_REVISION" > "$temporary_marker"
mv -- "$temporary_marker" "$MARKER"
trap - EXIT
