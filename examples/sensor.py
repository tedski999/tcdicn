import asyncio
import logging
import os
import random
import sys
import tcdicn


async def main():

    # Get parameters or defaults
    name = os.getenv("TCDICN_ID")  # A unique name to call me on the network
    port = int(os.getenv("TCDICN_PORT") or 33333)  # Listen on :33333
    dport = int(os.getenv("TCDICN_DPORT") or port)  # Talk to :33333
    ttl = int(os.getenv("TCDICN_TTL") or 30)  # Forget me after 30s
    tpf = int(os.getenv("TCDICN_TPF") or 3)  # Remind peers every 30/3s
    ttp = float(os.getenv("TCDICN_TTP") or 5)  # Repeat my adverts before 5s
    verb = os.getenv("TCDICN_VERBOSITY") or "info"  # Logging verbosity
    if name is None:
        sys.exit("Please give your sensor a unique ID by setting TCDICN_ID")

    # Logging verbosity
    verbs = {"dbug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARN}
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        level=verbs[verb], datefmt="%H:%M:%S")

    # Pick a random subset of labels to publish to
    labels = ["foo", "bar", "baz", "qux", "quux"]
    labels = random.sample(labels, random.randint(1, 3))
    labels.append("always")

    # ICN client node called name publishes these labels and needs
    # any data propagated back in under ttp seconds at each node
    client = {"name": name, "ttp": ttp, "labels": labels}
    node = tcdicn.Node()
    node_task = asyncio.create_task(node.start(port, dport, ttl, tpf, client))

    # Publish random data to a random label every couple of seconds
    async def run_sensor():
        while True:
            await asyncio.sleep(random.uniform(10, 30))
            label = random.choice(labels)
            value = random.randint(0, 10)
            logging.info("Publishing %s = %s...", label, value)
            try:
                await node.set(label, value)
            except OSError as exc:
                logging.error("Failed to publish: %s", exc)
    sensor_task = asyncio.create_task(run_sensor())

    # Run ICN node until shutdown while executing the sensor
    logging.info("Starting sensor...")
    tasks = [node_task, sensor_task]
    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    logging.info("Done.")

if __name__ == "__main__":
    asyncio.run(main())
