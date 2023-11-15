import asyncio
import logging
import os
import tcdicn


async def main():

    # Get parameters or defaults
    port = int(os.getenv("TCDICN_PORT") or 33333)
    dport = int(os.getenv("TCDICN_DPORT") or port)
    ttl = int(os.getenv("TCDICN_TTL") or 90)
    tpf = int(os.getenv("TCDICN_TPF") or 3)
    verb = os.getenv("TCDICN_VERBOSITY") or "INFO"

    # Logging verbosity
    verbs = {"DEBUG": logging.DEBUG, "INFO": logging.INFO}
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        level=verbs[verb], datefmt="%H:%M:%S:%m")

    # Run the ICN node until shutdown
    logging.info("Starting node...")
    await tcdicn.Node().start(port, dport, ttl, tpf)
    logging.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
