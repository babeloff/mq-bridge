#!/usr/bin/env bash
# Runs the Docker-backed CLI copy suite against the engine's integration stacks
# in tests/integration/docker-compose -- the same files, images and ports the
# engine's own integration tests use, so there is one definition per service.
#
# A service that is already up is reused and left running: the CLI tests then
# cost only their own runtime. Anything this script started, it stops again.
#
#   ./run_docker_tests.sh              # the default set, under a minute cold
#   ./run_docker_tests.sh all          # every backend
#   ./run_docker_tests.sh kafka nats   # only these
set -uo pipefail

cd "$(dirname "$0")"
REPO=$(cd ../../../../../.. && pwd)
COMPOSE_DIR="$REPO/tests/integration/docker-compose"

ALL=(postgres mysql mariadb kafka nats mongodb redis amqp mqtt)
# One SQL, one document store, one broker. The other six are opt-in here and
# run in parallel in CI, one backend per runner, so nothing loses coverage.
DEFAULT=(postgres mongodb nats)

# The port that says a service is already up. Compose files are named after the
# backend, so the file itself needs no mapping.
service_port() {
  case "$1" in
    postgres) echo 5432 ;; mysql) echo 3306 ;; mariadb) echo 3307 ;;
    kafka) echo 9092 ;;    nats) echo 4222 ;;  mongodb) echo 27017 ;;
    redis) echo 6379 ;;    amqp) echo 5672 ;;  mqtt) echo 1883 ;;
  esac
}

BACKENDS=("$@")
[ ${#BACKENDS[@]} -eq 0 ] && BACKENDS=("${DEFAULT[@]}")
[ "${BACKENDS[0]:-}" = "all" ] && BACKENDS=("${ALL[@]}")

# Build once so a slow first compile is not billed to a running container.
(cd "$REPO/apps/mq-bridge-app" && cargo test -p mq-bridge-app --test cli_copy_docker_test --no-run) || exit 1

log=$(mktemp)
trap 'rm -f "$log"' EXIT
failed=()
for backend in "${BACKENDS[@]}"; do
  started=$SECONDS
  file="$COMPOSE_DIR/$backend.yml"
  [ -f "$file" ] || { failed+=("$backend (no compose file)"); continue; }

  # Reuse whatever is already listening: an engine integration stack, or a
  # service left up on purpose between runs.
  if nc -z localhost "$(service_port "$backend")" 2>/dev/null; then
    reused=yes
  else
    reused=no
    if ! docker compose -f "$file" up -d --wait --wait-timeout 300 >/dev/null 2>&1; then
      failed+=("$backend (startup)")
      docker compose -f "$file" down -v >/dev/null 2>&1
      continue
    fi
  fi
  up=$((SECONDS - started))

  # The backend name is both the MQB_TEST_BACKEND filter and the cargo test-name
  # filter. Only the second one keeps the report honest: `backend!` skips by
  # returning, so without it every excluded test still reports `ok` and a run
  # against the wrong service looks green.
  (cd "$REPO/apps/mq-bridge-app" && MQB_TEST_BACKEND="$backend" \
    cargo test -p mq-bridge-app --test cli_copy_docker_test \
    -- --ignored --test-threads=1 "$backend") >"$log" 2>&1
  status=$?
  tail -n +2 "$log"

  # A filter that matches nothing still exits 0, so an empty run must not pass.
  ran=$(sed -n 's/^test result:.* \([0-9]*\) passed.*/\1/p' "$log" | head -1)
  if [ "$status" -ne 0 ]; then
    failed+=("$backend")
  elif [ "${ran:-0}" -eq 0 ]; then
    failed+=("$backend (no tests ran)")
  fi
  printf '==> %s: %s test(s), %ds startup (reused: %s), %ds total\n' \
    "$backend" "${ran:-0}" "$up" "$reused" "$((SECONDS - started))"

  [ "$reused" = no ] && docker compose -f "$file" down -v >/dev/null 2>&1
done

if [ ${#failed[@]} -ne 0 ]; then
  printf 'FAILED: %s\n' "${failed[@]}"
  exit 1
fi
echo "all backends passed"
