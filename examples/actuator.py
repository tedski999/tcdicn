import asyncio
import logging
import os
import random
import sys
import tcdicn


async def main():

    # Get parameters or defaults
    name = os.environ.get("TCDICN_ID")
    sport = int(os.environ.get("TCDICN_SPORT", 33333))
    dport = int(os.environ.get("TCDICN_DPORT", sport))
    ttl = float(os.environ.get("TCDICN_TTL", 90))
    tpf = int(os.environ.get("TCDICN_TPF", 6))
    ttp = float(os.environ.get("TCDICN_TTP", 1))
    get_ttl = float(os.environ.get("TCDICN_GET_TTL", 5))
    get_tpf = int(os.environ.get("TCDICN_GET_TPF", 1))
    get_ttp = float(os.environ.get("TCDICN_GET_TTP", 0))
    if name is None:
        sys.exit("Please give your sensor a unique ID by setting TCDICN_ID")

    # Logging verbosity
    logging.basicConfig(
        format="%(asctime)s.%(msecs)04d [%(levelname)s] %(message)s",
        level=logging.DEBUG, datefmt="%H:%M:%S:%m")

    # Pick a random subset of tags to subscribe to
    tags = ["foo", "bar", "baz", "qux", "quux"]
    tags = random.sample(tags, random.randint(1, 3))
    tags.append("always")

    # ICN client node called name does not publish and needs
    # any data propagated back in under ttp seconds at each node
    client = {"name": name, "ttp": ttp, "tags": []}
    node = tcdicn.Node()

    # Subscribe to random subset of data
    async def run_actuator():
        tasks = set()

        def subscribe(tag):
            logging.info("Subscribing to %s...", tag)
            getter = node.get(tag, get_ttl, get_tpf, get_ttp)
            task = asyncio.create_task(getter, name=tag)
            tasks.add(task)

        for tag in tags:
            subscribe(tag)

        while True:
            done, tasks = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                tag = task.get_name()
                value = task.result()
                logging.info("Received %s=%s", tag, value)
                subscribe(tag)

    # Run ICN node until shutdown while executing the actuator
    logging.info("Starting actuator...")
    actuator_task = asyncio.create_task(run_actuator())
    await node.start(sport, dport, ttl, tpf, client)
    actuator_task.cancel()
    logging.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
