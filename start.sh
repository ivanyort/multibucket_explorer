#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT_VALUE="${PORT:-8086}"

cd "$SCRIPT_DIR"

if [ ! -d node_modules ]; then
  echo "Instalando dependencias..."
  npm install
fi

echo "Subindo MultiBucket Explorer em http://localhost:${PORT_VALUE}"
PORT="$PORT_VALUE" npm start
