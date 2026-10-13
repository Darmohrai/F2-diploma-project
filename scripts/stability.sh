#!/usr/bin/env bash
# Runs the tests of the given modules several times in a row and stops at the first failure.
#
#   scripts/stability.sh <modules> <runs> [maven options]
#   scripts/stability.sh kurin-test,kurin-locks 20
#   scripts/stability.sh kurin-spring-boot-starter 20 -Pboot4
#
# The modules must be built first (./mvnw install -DskipTests): only the tests are run, offline, without
# recompiling. Each run uses one test JVM per module with a bounded heap (profile "stability";
# -Dkurin.test.heap=256m to change it). When a run fails, the surefire reports of that run are copied to
# stability-failures/<time>-run<N>/<module> and the script exits with status 1.
set -u

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <modules, comma separated> <runs> [maven options]" >&2
  exit 2
fi
modules="$1"
runs="$2"
shift 2

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root" || exit 2
log="$(mktemp)"
trap 'rm -f "$log"' EXIT

for run in $(seq 1 "$runs"); do
  if ./mvnw -o -q -Pstability "$@" -pl "$modules" surefire:test > "$log" 2>&1; then
    echo "run $run/$runs passed"
  else
    target="stability-failures/$(date +%Y%m%d-%H%M%S)-run$run"
    for module in $(echo "$modules" | tr ',' ' '); do
      if [ -d "$module/target/surefire-reports" ]; then
        mkdir -p "$target/$module"
        cp -r "$module/target/surefire-reports/." "$target/$module/"
      fi
    done
    cp "$log" "$target/maven.log"
    echo "run $run/$runs FAILED; reports and the Maven log are in $target" >&2
    grep -E 'Tests run:.*(FAIL|ERROR)|ERROR\]   ' "$log" | head -20 >&2
    exit 1
  fi
done
echo "$modules: $runs runs in a row passed"
