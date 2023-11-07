import asyncio
import json
import logging
import signal
import socket
from asyncio import StreamWriter, StreamReader, Task
from typing import Tuple, Dict

PORT = 33333
PEER_ANNOUNCEMENT = json.dumps({"version": "0.1", "type": "announcement"}).encode()
PEER_ANNOUNCEMENT_INTERVAL = 10
PEER_TIMEOUT_INTERVAL = 30

Address = Tuple[str, int]


class Server:
    # Initialise server
    # port: Which port number to listen and send requests on
    # peer_announcement: Bytes to send as peer announcement
    # peer_announcement_interval: How many seconds between each peer announcement
    # peer_timeout_interval: How many seconds without announcement until a peer is forgotten
    def __init__(self, port, peer_announcement, peer_announcement_interval, peer_timeout_interval):
        self.port = port
        self.peer_announcement = peer_announcement
        self.peer_announcement_interval = peer_announcement_interval
        self.peer_timeout_interval = peer_timeout_interval

        # Cache the address of peers whose announcement we have recently heard
        # Represented as a map from their address to their cache expiry timer task
        self.peers: Dict[Address, Task] = dict()

    # Start server
    async def start(self):
        try:
            self.udp_client: asyncio.DatagramTransport
            self.udp_server: "UdpServer"
            self.udp_client, self.udp_server = await self._start_udp_server()  # TODO unused variables
            self.tcp_server = await self._start_tcp_server()  # TODO unused variable
            logging.info(f"Listening on port :{self.port}")
            await asyncio.gather(self.udp_task, self.tcp_task)
        except asyncio.exceptions.CancelledError:
            logging.warning(f"Server tasks cancelled")

    # Retrieve named data from the ICN network
    async def get(self, name):
        return "TODO"

    # Publish named data to the ICN network
    async def set(self, name, value):
        return "TODO"

    # Start peer announcement listener and broadcaster
    async def _start_udp_server(self):
        logging.debug("Creating UDP server...")

        class UdpServer:
            def connection_made(udp, transport: asyncio.DatagramTransport):
                self.udp_task = asyncio.create_task(self._on_udp_connection(transport))

            def datagram_received(udp, data: bytes, addr: Address):
                asyncio.create_task(self._on_udp_datagram(data, addr))

        return await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: UdpServer(), local_addr=("0.0.0.0", self.port), allow_broadcast=True)

    # Regularly broadcast peer announcements to let other nodes know we exist
    async def _on_udp_connection(self, transport):
        while True:
            logging.debug("Sending peer announcement...")
            transport.sendto(self.peer_announcement, ("<broadcast>", self.port))
            await asyncio.sleep(self.peer_announcement_interval)

    # If we receive an announcement from a peer, add its address to our peer cache with an expiry timer
    # If it is already in our peer cache, reset its expiry timer
    async def _on_udp_datagram(self, data: bytes, addr: Address):
        if self._is_address_local(addr):
            return  # Ignore our own announcements
        if data != self.peer_announcement:
            logging.warning("Received malformed peer announcement; ignoring!")
            return  # Ignore malformed peer announcements
        logging.debug(f"Received peer announcement from {addr}.")
        if addr in self.peers:
            self.peers[addr].cancel()
        else:
            logging.info(f"Added new peer: {addr[0]}:{addr[1]}.")
        self.peers[addr] = asyncio.create_task(self._do_peer_timeout(addr))

    # Check if an address matches one of these nodes addresses
    # Adapted from https://gist.github.com/bennr01/7043a460155e8e763b3a9061c95faaa0
    def _is_address_local(self, addr: Address):
        hostname = socket.getfqdn(addr[0])
        if hostname in ("localhost", "0.0.0.0"):
            return True
        local_addrs = socket.getaddrinfo(socket.gethostname(), self.port)
        target_addrs = socket.getaddrinfo(hostname, addr[1])
        for (_, _, _, _, sock_addr) in local_addrs:
            for (_, _, _, _, r_sock_addr) in target_addrs:
                if r_sock_addr == sock_addr:
                    return True
        return False

    # A peer is expired from the cache if we do not hear an announcement from it for longer than "peer_timeout_interval"
    # As such, this coroutine will be cancelled and replaced with another if we hear another announcement, effectively
    # resetting the expiry timer
    async def _do_peer_timeout(self, addr: Address):
        await asyncio.sleep(self.peer_timeout_interval)
        logging.info(f"Removed timed-out peer: {addr[0]}:{addr[1]}.")
        del self.peers[addr]

    # Start listening for peer connecting to get or set named data on the network
    async def _start_tcp_server(self):
        logging.debug("Creating TCP server...")
        server = await asyncio.start_server(self._on_tcp_connection, None, self.port)
        self.tcp_task = asyncio.create_task(server.serve_forever())

    # Handle peer connection TODO
    async def _on_tcp_connection(self, reader: StreamReader, writer: StreamWriter):
        logging.info("Handling new TCP connection...")
        writer.close()


# Example of how to use the ICN server
async def main():
    # Debug logging verbosity
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.DEBUG)
    # Initialise server
    server = Server(PORT, PEER_ANNOUNCEMENT, PEER_ANNOUNCEMENT_INTERVAL, PEER_TIMEOUT_INTERVAL)
    server_task = asyncio.create_task(server.start())
    # Shutdown the ICN server if we receive any UNIX signals
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: server_task.cancel())
    # Start running server
    await server_task


if __name__ == "__main__":
    asyncio.run(main())
