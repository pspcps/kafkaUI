#!/usr/bin/env bash
set -e

echo "=== KafkaUI Setup ==="

# Check Go
if ! command -v go &>/dev/null; then
  echo "❌  Go is not installed. Install it:"
  echo "    brew install go   OR   https://go.dev/dl/"
  exit 1
fi
echo "✅  Go $(go version | awk '{print $3}')"

# Check Node
NODE_VER=$(node --version 2>/dev/null | cut -d. -f1 | tr -d 'v' || echo "0")
if [ "$NODE_VER" -lt 18 ]; then
  echo "❌  Node.js 18+ required (current: $(node --version 2>/dev/null || echo 'not found'))"
  echo "    Run: nvm install 20 && nvm use 20"
  exit 1
fi
echo "✅  Node $(node --version)"

# Backend deps
echo ""
echo "→  Installing Go dependencies..."
cd backend && go mod tidy
echo "✅  Go modules ready"
cd ..

# Frontend deps
echo ""
echo "→  Installing npm dependencies..."
cd frontend && npm install
echo "✅  npm packages installed"
cd ..

echo ""
echo "=== Setup complete! ==="
echo ""
echo "To start development:"
echo "  1. docker compose up zookeeper kafka -d   # start Kafka"
echo "  2. cd backend && go run ./cmd/server      # start API on :8080"
echo "  3. cd frontend && npm run dev             # start UI on :3000"
echo ""
echo "Or run everything with Docker:"
echo "  docker compose up --build"
