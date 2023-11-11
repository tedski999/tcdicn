import asyncio
import logging
import os
import random
import tcdicn


async def main():

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
    logging_format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(format=logging_format, level=logging.DEBUG)

    # Pick a random subset of tags to subscribe to
    tags = ["foo", "bar", "baz", "qux", "quux"]
    tags = random.sample(tags, random.randint(1, 3))

    # Start the client as a background task
    logging.info("Starting client...")
    client = tcdicn.Client(
        id, port, tags,
        server_host, server_port,
        net_ttl, net_tpf, net_ttp)

    # Subscribe to random subset of data
    async def run_actuator():
        tasks = []

        def subscribe(tag):
            getter = client.get(tag, get_ttl, get_tpf, get_ttp)
            task = asyncio.create_task(getter, name=tag)
            tasks.append(task)

        for tag in tags:
            logging.info(f"Subscribing to {tag}...")
            subscribe(tag)

        while True:
            done, tasks = await asyncio.wait(tasks)
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
