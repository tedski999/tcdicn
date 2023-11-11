import asyncio
import logging
import os
import random
import tcdicn


async def main():

    # Stagger starting ICN servers
    await asyncio.sleep(random.uniform(0, 5))

    # Get parameters or defaults
    port = int(os.environ.get("PORT", 33333))
    net_ttl = int(os.environ.get("NET_TTL", 180))
    net_tpf = int(os.environ.get("NET_TPF", 3))

    # Logging verbosity
    logging.basicConfig(
        format="%(asctime)s.%(msecs)04d [%(levelname)s] %(message)s",
        level=logging.INFO, datefmt="%H:%M:%S:%m")

    # Start the server as a background task
    logging.info("Starting server...")
    server = tcdicn.Server(port, net_ttl, net_tpf)

    # Wait for the server to shutdown
    try:
        await server.task
    except asyncio.exceptions.CancelledError:
        logging.info("Server has shutdown.")


if __name__ == "__main__":
    asyncio.run(main())
