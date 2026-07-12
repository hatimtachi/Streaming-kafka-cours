#!/usr/bin/env bash
# Lance la suite de tests (TopologyTestDriver, sans cluster).
set -euo pipefail
cd "$(dirname "$0")"
echo "== Tests (TopologyTestDriver, sans cluster) =="
mvn -q test
