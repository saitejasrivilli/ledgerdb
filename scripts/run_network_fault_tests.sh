#!/usr/bin/env bash
# Runs tests/networkfault/... for real, locally, on macOS/Windows dev
# machines that have no native iptables/tc — via a privileged Linux
# Docker container (--cap-add=NET_ADMIN --privileged), the same pattern
# CI uses natively on ubuntu-latest. See docs/design_network_fault.md.
set -euo pipefail

cd "$(dirname "$0")/.."

docker run --rm \
  --cap-add=NET_ADMIN \
  --privileged \
  -e NETFAULT_TEST=1 \
  -v "$(pwd)":/src \
  -w /src \
  golang:1.26 \
  bash -c "apt-get update -qq && apt-get install -y -qq iptables iproute2 >/dev/null && go test ./tests/networkfault/... -v -count=1"
