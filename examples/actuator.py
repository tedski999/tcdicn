import asyncio
import logging
import os
import random
import sys
import tcdicn


async def main():

    # Get parameters or defaults
    name = os.getenv("TCDICN_ID")
    port = int(os.getenv("TCDICN_PORT") or 33333)
    dport = int(os.getenv("TCDICN_DPORT") or port)
    ttl = float(os.getenv("TCDICN_TTL") or 90)
    tpf = int(os.getenv("TCDICN_TPF") or 3)
    ttp = float(os.getenv("TCDICN_TTP") or 1)
    get_ttl = float(os.getenv("TCDICN_GET_TTL") or 90)
    get_tpf = int(os.getenv("TCDICN_GET_TPF") or 2)
    get_ttp = float(os.getenv("TCDICN_GET_TTP") or 0)
    verb = os.getenv("TCDICN_VERBOSITY") or "INFO"
    if name is None:
        sys.exit("Please give your sensor a unique ID by setting TCDICN_ID")

    # Logging verbosity
    verbs = {"DEBUG": logging.DEBUG, "INFO": logging.INFO}
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        level=verbs[verb], datefmt="%H:%M:%S")

    # Pick a random subset of labels to subscribe to
    labels = ["foo", "bar", "baz", "qux", "quux"]
    labels = random.sample(labels, random.randint(1, 3))
    labels.append("always")

    # ICN client node called name does not publish and needs
    # any data propagated back in under ttp seconds at each node
    client = {"name": name, "ttp": ttp, "labels": []}
    node = tcdicn.Node()
    node_task = asyncio.create_task(node.start(port, dport, ttl, tpf, client))

    # Subscribe to random subset of data
    async def run_actuator():
        tasks = set()

        def subscribe(label):
            logging.info("Subscribing to %s...", label)
            getter = node.get(label, get_ttl, get_tpf, get_ttp)
            task = asyncio.create_task(getter, name=label)
            tasks.add(task)

        for label in labels:
            subscribe(label)

        while True:
            done, tasks = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                label = task.get_name()
                value = task.result()
                logging.info("Received %s=%s", label, value)
                subscribe(label)
    actuator_task = asyncio.create_task(run_actuator())

    # Run ICN node until shutdown while executing the actuator
    logging.info("Starting actuator...")
    tasks = [node_task, actuator_task]
    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    actuator_task.cancel()
    logging.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
