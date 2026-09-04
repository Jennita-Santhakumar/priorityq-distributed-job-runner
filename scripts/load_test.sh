#!/usr/bin/env bash
# Runs the Go load test tool against a running local docker compose stack
# and prints the real measured numbers used in README's Design Decisions.
set -euo pipefail
cd "$(dirname "$0")/.."
go run ./scripts/loadtest -url "${1:-http://localhost:8030}" -n "${2:-2000}" -c "${3:-50}"
