Requires python3.8 and asyncio.

Run the example server:

    PYTHONPATH=. python3 ./examples/server.py

Clients should have unique names on the network. Run the example sensor:

    PYTHONPATH=. TCDICN_ID=my_cool_sensor python3 ./examples/sensor.py

If you would like to test locally with virtual ICN nodes, run one of the example scenarios using Docker:

    TCDICN_TTL=10 TCDICN_GET_TTL=30 TCDICN_VERBOSITY=INFO docker compose --file simulations/paths.yml up --build
