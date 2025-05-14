#!/usr/bin/env bash
# Use Bash interpreter, exit on errors or undefined variables, and handle pipelines safely.
set -euo pipefail
IFS=$'\n\t'

# Define a map from local folder names to Docker Compose project names
declare -A SERVICES=(
  [databases]="db"
  [apache-airflow]="airflow"
  [api]="api"
  [streamlit]="streamlit"
)

# Set paths to the two CSV data files and the Python generator script
ONLINE_STORE_CSV="$(pwd)/databases/init-scripts/online_store/online_store.csv"
PHYSICAL_STORE_CSV="$(pwd)/databases/init-scripts/physical_store/physical_store.csv"
PYTHON_INIT_SCRIPT="$(pwd)/databases/dataset/script.py"

# If either CSV file is missing, generate them by running the Python script
if [ ! -e "$ONLINE_STORE_CSV" ] || [ ! -e "$PHYSICAL_STORE_CSV" ]; then
    echo "⚠️  Missing CSV files, generating with Python..."
    # Check that python3 is installed
    command -v python3 >/dev/null 2>&1 || {
        echo "✖️  python3 not found in PATH" >&2
        exit 1
    }
    # Run the Python script; on failure, exit with error
    if ! python3 "$PYTHON_INIT_SCRIPT"; then
        echo "✖️  Failed to execute $PYTHON_INIT_SCRIPT" >&2
        exit 1
    fi
    echo "✅ CSV files generated successfully."
else
    echo "✅ CSV files already exist."
fi

# Point to the shared environment variables file
ENV_FILE="$(pwd)/.env"

# Turn off Docker’s content trust (image signature verification)
export DOCKER_CONTENT_TRUST=0

# Set a fixed UID for Airflow files to avoid permission issues
AIRFLOW_UID=1000
export AIRFLOW_UID

# Create a Docker network named “store_network” if it doesn’t already exist
if ! docker network inspect store_network >/dev/null 2>&1; then
  echo "🔧 Creating external network store_network"
  docker network create --driver bridge store_network
fi

# For each service folder, spin up its containers with Docker Compose
for DIR in "${!SERVICES[@]}"; do
  PROJECT="${SERVICES[$DIR]}"

  echo "🚀 Starting $DIR (project: $PROJECT)..."
  (
    cd "$DIR"
    # Pass in UID and disable content trust again, then bring up containers in detached mode
    AIRFLOW_UID="$AIRFLOW_UID" \
    DOCKER_CONTENT_TRUST=0 \
    docker-compose \
      --env-file "$ENV_FILE" \
      up -d
  ) &
done

# Wait for all background “docker-compose up” processes to finish
wait
echo "✅ All services are up."
