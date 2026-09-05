#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
: "${UTOPIA_CODEX_HOME_HOST:?set UTOPIA_CODEX_HOME_HOST to an absolute dedicated credential directory}"

case "$UTOPIA_CODEX_HOME_HOST" in
  /*) ;;
  *)
    printf '%s\n' 'UTOPIA_CODEX_HOME_HOST must be an absolute path' >&2
    exit 2
    ;;
esac

host_path=$(realpath -m -- "$UTOPIA_CODEX_HOME_HOST")
case "$host_path" in
  "$repo_root"|"$repo_root"/*)
    printf '%s\n' 'UTOPIA_CODEX_HOME_HOST must resolve outside the repository' >&2
    exit 2
    ;;
esac

export UTOPIA_CODEX_HOME_HOST="$host_path"
exec docker compose \
  -f "$repo_root/docker-compose.yml" \
  -f "$repo_root/docker-compose.codex.yml" \
  "$@"
