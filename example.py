import asyncio
import json
import logging
import random
import signal
import tcdicn

PORT = 33333
ANNOUNCEMENT = json.dumps({"version": "0.1", "type": "announcement"}).encode()
ANNOUNCEMENT_INTERVAL = 10
PEER_TIMEOUT_INTERVAL = 30


# Example scenario to randomly get and set named data
async def scenario(server):
    while True:
        await asyncio.sleep(random.uniform(5, 30))
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
    # Debug logging verbosity
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.DEBUG)
    # Initialise server
    server = tcdicn.Server(PORT, ANNOUNCEMENT, ANNOUNCEMENT_INTERVAL, PEER_TIMEOUT_INTERVAL)
    # Shutdown the ICN server if we receive any UNIX signals
    for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, lambda: server_task.cancel())
    # Run until one of the tasks complete
    server_task = asyncio.create_task(server.start())
    scenario_task = asyncio.create_task(scenario(server))
    _, tasks = await asyncio.wait([server_task, scenario_task], return_when=asyncio.FIRST_COMPLETED)
    # Cancel any remaining tasks
    for task in tasks:
        task.cancel()
    await asyncio.wait(tasks)


if __name__ == "__main__":
    asyncio.run(main())
