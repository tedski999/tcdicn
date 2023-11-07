import asyncio
import json
import logging
import signal
import socket
from asyncio import DatagramTransport, StreamWriter, StreamReader, Task
from typing import Tuple, Dict

# Default values - You should probably use these if you want to be compatible with other nodes!
PORT = 33333
ANNOUNCEMENT = json.dumps({"version": "0.1", "type": "announcement"}).encode()
ANNOUNCEMENT_INTERVAL = 10
PEER_TIMEOUT_INTERVAL = 30


class Server:

    # Initialise server
    def __init__(self, port: int, announcement: bytes, announcement_interval: int, peer_timeout_interval: int):
        self.port = port  # Port number to listen on and send requests to
        self.announcement = announcement  # Bytes to send as announcement
        self.announcement_interval = announcement_interval  # Seconds between each announcement
        self.peer_timeout_interval = peer_timeout_interval  # Seconds without announcement until a peer is forgotten
        self.peers: Dict[Tuple[str, int], Task] = dict()  # Cache of peers whose announcement we have recently heard

    # Start server
    async def start(self):
        logging.info(f"Listening on port :{self.port}")
        try:
            await asyncio.gather(self._start_udp_server(), self._start_tcp_server())
        except OSError as e:
            logging.error(f"Server error: {e}")
        except asyncio.exceptions.CancelledError:
            logging.warning(f"Server tasks cancelled")

    # Retrieve named data from the ICN network
    async def get(self, name: str):
        return "TODO"

    # Publish named data to the ICN network
    async def set(self, name: str, value: str):
        return "TODO"

    # Listen for and regularly broadcast peer announcements to let other nodes know we exist
    async def _start_udp_server(self):
        logging.debug("Creating UDP server...")
        # Listen for announcements
        class Protocol:
            def connection_made(_, transport: DatagramTransport):
                logging.debug(f"UDP transport established: {transport}")
            def connection_lost(_, e: Exception):
                logging.warning(f"UDP transport lost: {e}")
            def datagram_received(_, data: bytes, addr: Tuple[str, int]):
                self._on_udp_datagram(data, *addr)
            def error_received(_, e: OSError):
                logging.warning(f"UDP transport error: {e}")
        transport, protocol = await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: Protocol(), local_addr=("0.0.0.0", self.port), allow_broadcast=True)
        # Regularly broadcast announcements
        while True:
            logging.debug("Sending peer announcement...")
            transport.sendto(self.announcement, ("<broadcast>", self.port))
            await asyncio.sleep(self.announcement_interval)

    # If we receive an announcement from a peer, add its address to our peer cache with an expiry timer
    # If it is already in our peer cache, reset its expiry timer
    def _on_udp_datagram(self, data: bytes, host: str, port: int):
        # Ignore own announcements, adapted from https://gist.github.com/bennr01/7043a460155e8e763b3a9061c95faaa0
        local_addrs = socket.getaddrinfo(socket.gethostname(), self.port)
        remote_addrs = socket.getaddrinfo(socket.getfqdn(host), port)
        for (_, _, _, _, local_addr) in local_addrs:
            for (_, _, _, _, remote_addr) in remote_addrs:
                if remote_addr == local_addr:
                    return
        # Ignore malformed announcements
        if data != self.announcement:
            return
        # Cancel previous expiry timer
        logging.debug(f"Received peer announcement from {host}:{port}.")
        if (host, port) in self.peers:
            self.peers[(host, port)].cancel()
        else:
            logging.info(f"Added new peer: {host}:{port}.")
        # Insert new peer address and expiry timer
        async def _do_peer_timeout():
            await asyncio.sleep(self.peer_timeout_interval)
            logging.info(f"Removed timed-out peer: {host}:{port}.")
            del self.peers[(host, port)]
        self.peers[(host, port)] = asyncio.create_task(_do_peer_timeout())

    # Start listening for peer connecting to get or set named data on the network
    async def _start_tcp_server(self):
        logging.debug("Creating TCP server...")
        server = await asyncio.start_server(self._on_tcp_connection, None, self.port)
        await server.serve_forever()

    # Handle peer connection TODO
    async def _on_tcp_connection(self, reader: StreamReader, writer: StreamWriter):
        logging.info("Handling new TCP connection...")
        writer.close()


# Example of how to use the ICN server
async def main():
    # Debug logging verbosity
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.DEBUG)
    # Initialise server
    server = Server(PORT, ANNOUNCEMENT, ANNOUNCEMENT_INTERVAL, PEER_TIMEOUT_INTERVAL)
    server_task = asyncio.create_task(server.start())
    # Shutdown the ICN server if we receive any UNIX signals
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: server_task.cancel())
    # Start running server
    await server_task


if __name__ == "__main__":
    asyncio.run(main())
