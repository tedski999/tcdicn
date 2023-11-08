import asyncio
import logging
import random
import signal
import sys
import tcdicn

# Default values
PORT = 33333
ANNOUNCE_INTERVAL = 60
PEER_TIMEOUT = 180


# Example scenario to randomly get and set named data
async def scenario(server: tcdicn.Server):
    while True:
        await asyncio.sleep(random.uniform(30, 180))
        name = random.choice(["foo", "bar", "baz"])
        if random.choice([True, False]):
            value = random.choice(range(0, 10))
            logging.info(f"Setting {name} to {value}...")
            await server.set(name, value)
        else:
            logging.info(f"Getting {name}...")
            value = await server.get(name)
            logging.info(f"{name} is {value}")


async def main():
    # Parse CLI arguments
    port = sys.argv[1] if len(sys.argv) > 1 else PORT
    announce_interval = sys.argv[2] if len(sys.argv) > 2 else ANNOUNCE_INTERVAL
    peer_timeout = sys.argv[3] if len(sys.argv) > 3 else PEER_TIMEOUT
    # Debug logging verbosity
    logging_format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(format=logging_format, level=logging.INFO)
    # Initialise server
    server = tcdicn.Server(port, announce_interval, peer_timeout)
    # Shutdown the ICN server if we receive any UNIX signals
    loop = asyncio.get_running_loop()
    sigs = [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]
    [loop.add_signal_handler(s, lambda: server_task.cancel()) for s in sigs]
    # Run until one of the tasks complete
    server_task = asyncio.create_task(server.start())
    scenario_task = asyncio.create_task(scenario(server))
    tasks = [server_task, scenario_task]
    _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    # Cancel any remaining tasks
    [task.cancel() for task in pending]
    await asyncio.wait(pending)


if __name__ == "__main__":
    asyncio.run(main())
