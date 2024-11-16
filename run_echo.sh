
NUM_NODES=4
# python -m cs4545.system.util compose $NUM_NODES topologies/echo.yaml echo
python -m cs4545.system.util compose $NUM_NODES topologies/echo.yaml echo

# Exit if the above command fails
if [ $? -ne 0 ]; then
    exit 1
fi

docker compose build
docker compose up