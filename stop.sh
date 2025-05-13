#!/usr/bin/env bash
set -euo pipefail

# Map directory names to unique Docker Compose project names
declare -A SERVICES=(
  [databases]="db"
  [apache-airflow]="airflow"
  [api]="api"
  [streamlit]="streamlit"
)

for DIR in "${!SERVICES[@]}"; do
  PROJECT="${SERVICES[$DIR]}"

  echo "🛑 Stopping $DIR (project: $PROJECT)..."
  (
    cd "$DIR"
    # Bring down all containers, networks, and default volumes for this project
    docker-compose down
  ) &
done

wait
echo "✅ All services have been stopped."
