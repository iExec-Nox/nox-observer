#!/usr/bin/env bash
#
# Run nox-observer natively against local dependencies.
#
# Loads .env, points the database host at localhost, and forwards
# any arguments to the binary.
# e.g.:
#   ./run.sh
#   ./run.sh <arg>
#

set -euo pipefail

# Always run from the repo root (where .env and Cargo.toml live), regardless of
# the caller's working directory.
cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
  echo "error: .env not found"
  exit 1
fi

# Export every var defined in .env into the environment.
set -a
source .env
set +a

export NOX_OBSERVER_DATABASE__HOST=localhost
export RUST_LOG="${RUST_LOG:-info}"

# exec so signals (Ctrl-C) reach cargo/the binary directly.
exec cargo run -- "$@"
