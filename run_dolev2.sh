#!/bin/bash

# Number of nodes in the network
NUM_NODES=4  # Cambia a 10 per riflettere la topologia `dolev.yaml`

# Generate the topology and compose file for Dolev's Algorithm
python -m cs4545.system.util compose $NUM_NODES topologies/dolev2.yaml dolev

# Exit if the above command fails
if [ $? -ne 0 ]; then
    echo "Error during topology generation or configuration"
    exit 1
fi

# Build the Docker images
docker compose build

# Start the distributed network
docker compose up
