#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT_VALUE="${PORT:-8086}"

cd "$SCRIPT_DIR"

if [ ! -d node_modules ]; then
  echo "Installing dependencies..."
  npm install
fi

if [ "${ALLOW_INSECURE_HTTP:-false}" != "true" ]; then
  if [ -z "${TLS_CERT_FILE:-}" ]; then
    echo "TLS_CERT_FILE is required unless ALLOW_INSECURE_HTTP=true." >&2
    exit 1
  fi

  if [ -z "${TLS_KEY_FILE:-}" ]; then
    echo "TLS_KEY_FILE is required unless ALLOW_INSECURE_HTTP=true." >&2
    exit 1
  fi
fi

if [ "${ALLOW_INSECURE_HTTP:-false}" = "true" ]; then
  echo "Starting MultiBucket Explorer in insecure HTTP mode at http://localhost:${PORT_VALUE}"
else
  echo "Starting MultiBucket Explorer at https://localhost:${PORT_VALUE}"
fi

PORT="$PORT_VALUE" npm start
