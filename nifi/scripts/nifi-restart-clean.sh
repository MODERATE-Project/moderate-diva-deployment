#!/usr/bin/env bash
# Restart NiFi and clear transient repository data (queues, content, provenance, Python work).
# Preserves flow configuration (database_repository, state) and python_extensions.
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly NIFI_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DATA_DIR="${NIFI_ROOT}/nifi"

log() {
  printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

clean_dir_contents() {
  local target_dir="$1"
  if [[ ! -d "${target_dir}" ]]; then
    log "Skip missing directory: ${target_dir}"
    return 0
  fi
  log "Cleaning ${target_dir}"
  find "${target_dir}" -mindepth 1 -delete
}

main() {
  cd "${NIFI_ROOT}"

  log "Stopping NiFi container"
  docker compose stop --timeout 120 nifi

  log "Cleaning repositories"
  clean_dir_contents "${DATA_DIR}/flowfile_repository"
  clean_dir_contents "${DATA_DIR}/content_repository"
  clean_dir_contents "${DATA_DIR}/provenance_repository"
  clean_dir_contents "${DATA_DIR}/work/python"

  log "Starting NiFi container"
  docker compose up -d nifi

  log "Applying nifi.properties from mount (if container is up)"
  sleep 5
  if docker compose ps --status running --format '{{.Name}}' | grep -q nifi; then
    docker compose exec -T nifi cp -a /tmp/nifi.properties \
      /opt/nifi/nifi-current/conf/nifi.properties || true
    docker compose restart nifi
  fi

  log "Done. Tail logs: docker compose -f ${NIFI_ROOT}/docker-compose.yml logs -f nifi"
}

main "$@"
