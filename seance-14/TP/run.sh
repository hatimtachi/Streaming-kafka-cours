#!/usr/bin/env bash
# Compile et lance l'application du TP (necessite JDK 21 + Maven).
set -euo pipefail
cd "$(dirname "$0")"
echo "== Application : compilation + execution =="
mvn -q compile exec:java -Dexec.mainClass=fr.esgi.kafka.tp14.CountApiApp
