import asyncio
import logging
import os
import tcdicn


async def main():

    # Get parameters or defaults
    port = int(os.getenv("TCDICN_PORT") or 33333)  # Listen on :33333
    dport = int(os.getenv("TCDICN_DPORT") or port)  # Talk to :33333
    ttl = int(os.getenv("TCDICN_TTL") or 30)  # Forget me after 30s
    tpf = int(os.getenv("TCDICN_TPF") or 3)  # Remind peers every 30/3s
    verb = os.getenv("TCDICN_VERBOSITY") or "info"  # Logging verbosity

    # Logging verbosity
    verbs = {"dbug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARN}
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        level=verbs[verb], datefmt="%H:%M:%S")

    # Run the ICN node until shutdown
    logging.info("Starting node...")
    await tcdicn.Node().start(port, dport, ttl, tpf)
    logging.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
