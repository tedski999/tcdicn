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
    keyfile = os.getenv("TCDICN_KEYFILE") or None  # Private keyfile path
    trusteds = os.getenv("TCDICN_TRUSTEDS") or None  # Trusted client paths
    group = os.getenv("TCDICN_GROUP") or None  # Which group to make/join
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
    client = {"name": name, "ttp": ttp, "labels": labels if group is None else []}

    # Load this clients private key
    if keyfile is not None:
        with open(keyfile, "rb") as f:
            client["key"] = f.read()

    # Start ICN node as a client
    node = tcdicn.Node()
    node_task = asyncio.create_task(node.start(port, dport, ttl, tpf, client))

    # Join every trusted client in a group
    if any([trusteds, group, keyfile]):
        if not all([trusteds, group, keyfile]):
            raise RuntimeError("Missing some group config")
        tasks = [node_task]
        for trusted in trusteds.split(","):
            name = os.path.basename(trusted)
            with open(trusted, "rb") as f:
                key = f.read()
            tasks.append(asyncio.create_task(node.join(group, name, key, labels)))
        while len(tasks) > 1:
            done, tasks = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED)
            if node_task in done:
                return

    # Publish random data to a random label every couple of seconds
    async def run_sensor():
        while True:
            await asyncio.sleep(random.uniform(10, 30))
            label = random.choice(labels)
            value = str(random.randint(0, 10))
            logging.info("Publishing %s = %s...", label, value)
            try:
                await node.set(label, value, group)
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
