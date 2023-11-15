import asyncio
import logging
import os
import tcdicn


async def main():

    # Get parameters or defaults
    port = int(os.environ.get("TCDICN_PORT", 33333))
    dport = int(os.environ.get("TCDICN_DPORT", port))
    ttl = int(os.environ.get("TCDICN_TTL", 180))
    tpf = int(os.environ.get("TCDICN_TPF", 6))

    # Logging verbosity
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        level=logging.DEBUG, datefmt="%H:%M:%S:%m")

    # Run the ICN node until shutdown
    logging.info("Starting node...")
    await tcdicn.Node().start(port, dport, ttl, tpf)
    logging.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())