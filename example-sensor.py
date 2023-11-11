import asyncio
import logging
import os
import random
import tcdicn


async def main():

    # Allow the ICN server to start before attempting to use it
    await asyncio.sleep(random.uniform(5, 8))

    # Get parameters or defaults
    id = os.environ.get("ID", "my_cool_sensor")
    port = int(os.environ.get("PORT", random.randint(33334, 65536)))
    server_host = os.environ.get("SERVER_HOST", "localhost")
    server_port = int(os.environ.get("SERVER_PORT", 33333))
    net_ttl = int(os.environ.get("NET_TTL", 180))
    net_tpf = int(os.environ.get("NET_TPF", 3))
    net_ttp = float(os.environ.get("NET_TTP", 0))

    # Logging verbosity
    logging.basicConfig(
        format="%(asctime)s.%(msecs)04d [%(levelname)s] %(message)s",
        level=logging.INFO, datefmt="%H:%M:%S:%m")

    # Pick a random subset of tags to publish to
    tags = ["foo", "bar", "baz", "qux", "quux"]
    tags = random.sample(tags, random.randint(1, 3))
    tags.append("always")

    # Start the client as a background task
    logging.info("Starting client...")
    client = tcdicn.Client(
        id, port, tags,
        server_host, server_port,
        net_ttl, net_tpf, net_ttp)

    # Publish random data to a random tag every couple of seconds
    async def run_sensor():
        while True:
            await asyncio.sleep(random.uniform(1, 2))
            tag = random.choice(tags)
            value = random.randint(0, 10)
            logging.info(f"Publishing {tag}={value}...")
            try:
                await client.set(tag, value)
            except OSError as e:
                logging.error(f"Failed to publish: {e}")

    # Initialise execution of the sensor logic as a coroutine
    logging.info("Starting sensor...")
    sensor = run_sensor()

    # Wait for the client to shutdown while executing the sensor coroutine
    both_tasks = asyncio.gather(client.task, sensor)
    try:
        await both_tasks
    except asyncio.exceptions.CancelledError:
        logging.info("Client has shutdown.")


if __name__ == "__main__":
    asyncio.run(main())
