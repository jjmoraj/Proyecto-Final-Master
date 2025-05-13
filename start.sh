#!/usr/bin/env bash
set -euo pipefail

# Map directory names to unique Docker Compose project names
declare -A SERVICES=(
  [databases]="db"
  [apache-airflow]="airflow"
  [api]="api"
  [streamlit]="streamlit"
)

# Path to the shared .env file
ENV_FILE="$(pwd)/.env"

# Disable Docker Content Trust for all image pulls globally
export DOCKER_CONTENT_TRUST=0

AIRFLOW_UID=1000
export AIRFLOW_UID

if ! docker network inspect store_network >/dev/null 2>&1; then
  echo "🔧 Creating external network store_network"
  docker network create --driver bridge store_network
fi


for DIR in "${!SERVICES[@]}"; do
  PROJECT="${SERVICES[$DIR]}"

  echo "🚀 Starting $DIR (project: $PROJECT)..."
  (
    cd "$DIR"
    AIRFLOW_UID="$AIRFLOW_UID" \
    DOCKER_CONTENT_TRUST=0 \
    docker-compose \
      --env-file "$ENV_FILE" \
      up -d
  ) &
done

wait
echo "✅ All services are up."
