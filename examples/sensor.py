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
    if name is None:
        sys.exit("Please give your sensor a unique ID by setting TCDICN_ID")

    # Logging verbosity
    logging.basicConfig(
        format="%(asctime)s.%(msecs)04d [%(levelname)s] %(message)s",
        level=logging.DEBUG, datefmt="%H:%M:%S:%m")

    # Pick a random subset of tags to publish to
    tags = ["foo", "bar", "baz", "qux", "quux"]
    tags = random.sample(tags, random.randint(1, 3))
    tags.append("always")

    # ICN client node called name publishes these tags and needs
    # any data propagated back in under ttp seconds at each node
    client = {"name": name, "ttp": ttp, "tags": tags}
    node = tcdicn.Node()

    # Publish random data to a random tag every couple of seconds
    async def run_sensor():
        while True:
            await asyncio.sleep(random.uniform(1, 2))
            tag = random.choice(tags)
            value = random.randint(0, 10)
            logging.info("Publishing %s = %s...", tag, value)
            try:
                await node.set(tag, value)
            except OSError as exc:
                logging.error("Failed to publish: %s", exc)

    # Run ICN node until shutdown while executing the sensor
    logging.info("Starting sensor...")
    sensor_task = asyncio.create_task(run_sensor())
    await node.start(sport, dport, ttl, tpf, client)
    sensor_task.cancel()
    logging.info("Done.")

if __name__ == "__main__":
    asyncio.run(main())
