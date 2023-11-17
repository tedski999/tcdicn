import asyncio
import logging
import os
import tcdicn


async def main():

    # Get parameters or defaults
    port = int(os.getenv("TCDICN_PORT") or 33333)  # Listen on :33333
    dport = int(os.getenv("TCDICN_DPORT") or port)  # Talk to :33333
    wport = int(os.getenv("TCDICN_WPORT") or None)  # Debug web server port
    ttl = int(os.getenv("TCDICN_TTL") or 30)  # Forget me after 30s
    tpf = int(os.getenv("TCDICN_TPF") or 3)  # Remind peers every 30/3s
    verb = os.getenv("TCDICN_VERBOSITY") or "info"  # Logging verbosity

    # Logging verbosity
    verbs = {"dbug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARN}
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        level=verbs[verb], datefmt="%H:%M:%S")

    # Start ICN node as a client
    node = tcdicn.Node()
    node_task = asyncio.create_task(node.start(port, dport, ttl, tpf))

    # Serve debug information if requested
    if wport is not None:
        debug_task = asyncio.create_task(node.serve_debug(wport))

    # Run the ICN node until shutdown
    logging.info("Starting node...")
    await tcdicn.Node().start(port, dport, ttl, tpf)
    logging.info("Done.")

    # Stop everything
    if node_task is not None:
        node_task.cancel()
    if debug_task is not None:
        debug_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
