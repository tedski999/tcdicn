import asyncio
import logging
import os
import random
import tcdicn


async def main():

    # Allow the ICN server to start before attempting to use it
    await asyncio.sleep(random.uniform(5, 8))

    # Get parameters or defaults
    id = os.environ.get("ID", "my_cool_actuator")
    port = int(os.environ.get("PORT", random.randint(33334, 65536)))
    server_host = os.environ.get("SERVER_HOST", "localhost")
    server_port = int(os.environ.get("SERVER_PORT", 33333))
    net_ttl = int(os.environ.get("NET_TTL", 180))
    net_tpf = int(os.environ.get("NET_TPF", 3))
    net_ttp = float(os.environ.get("NET_TTP", 0))
    get_ttl = int(os.environ.get("GET_TTL", 180))
    get_tpf = int(os.environ.get("GET_TPF", 3))
    get_ttp = float(os.environ.get("GET_TTP", 0))

    # Logging verbosity
    logging.basicConfig(
        format="%(asctime)s.%(msecs)04d [%(levelname)s] %(message)s",
        level=logging.INFO, datefmt="%H:%M:%S:%m")

    # Pick a random subset of tags to subscribe to
    tags = ["foo", "bar", "baz", "qux", "quux"]
    tags = random.sample(tags, random.randint(1, 3))
    tags.append("always")

    # Start the client as a background task
    logging.info("Starting client...")
    client = tcdicn.Client(
        id, port, [],
        server_host, server_port,
        net_ttl, net_tpf, net_ttp)

    # Subscribe to random subset of data
    async def run_actuator():
        tasks = set()

        def subscribe(tag):
            getter = client.get(tag, get_ttl, get_tpf, get_ttp)
            task = asyncio.create_task(getter, name=tag)
            tasks.add(task)

        for tag in tags:
            logging.info(f"Subscribing to {tag}...")
            subscribe(tag)

        while True:
            done, tasks = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                tag = task.get_name()
                value = task.result()
                logging.info(f"Received {tag}={value}")
                logging.info(f"Resubscribing to {tag}...")
                subscribe(tag)

    # Initialise execution of the actuator logic as a coroutine
    logging.info("Starting actuator...")
    actuator = run_actuator()

    # Wait for the client to shutdown while executing the actuator coroutine
    both_tasks = asyncio.gather(client.task, actuator)
    try:
        await both_tasks
    except asyncio.exceptions.CancelledError:
        logging.info("Client has shutdown.")


if __name__ == "__main__":
    asyncio.run(main())
