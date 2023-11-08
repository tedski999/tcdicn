import asyncio
import json
import logging
import socket
import time
from asyncio import DatagramTransport, StreamWriter, StreamReader, Task
from typing import List, Tuple, Dict


VERSION = "0.1"


# Utility class for storing the address of peers in a useful structure
class _Address:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def __eq__(self, other):
        return self.host == other.host and self.port == other.port

    def __hash__(self):
        return hash((self.host, self.port))

    def __str__(self):
        return f"{self.host}:{self.port}"


# Provides all the networking logic for interacting with a network of ICN nodes
class Server:

    # Initialise server
    def __init__(self, port: int, announce_interval: int, peer_timeout: int):
        self.port = port
        self.announcement = self._create_announcement_msg()
        self.announce_interval = announce_interval
        self.peer_timeout = peer_timeout
        self.peers: Dict[_Address, Task] = dict()
        self.dataset = dict()

    # Start the server as a coroutine
    # Cancel the coroutine to shutdown the server gracefully
    async def start(self):
        logging.info(f"Listening on port :{self.port}")
        try:
            await asyncio.gather(self._start_udp(), self._start_tcp())
        except OSError as e:
            logging.error(f"Server error: {e}")
        except asyncio.exceptions.CancelledError:
            logging.warning("Server tasks cancelled")

    # Retrieve named data from the ICN network
    # Does not actually do any networking, just looks up what is in local cache
    async def get(self, name: str):
        return self.dataset[name]["value"] if name in self.dataset else "?"

    # Publish named data to the ICN network
    # The data is pushed to all peers, who push to all their peers, etc...
    async def set(self, name: str, value: str):
        self.dataset[name] = {"value": value, "time": time.time()}
        await self._send_to_addrs(self._create_set_msg(name), self.peers)

    # Methods below here are "private" and should not be used by applications

    # Listen for and regularly broadcast peer announcements to other nodes
    async def _start_udp(self):
        logging.debug("Creating UDP server...")

        # Listen for announcements
        class Protocol:
            def connection_made(_, transport: DatagramTransport):
                logging.debug(f"UDP transport established: {transport}")

            def connection_lost(_, e: Exception):
                logging.warning(f"UDP transport lost: {e}")

            def datagram_received(_, data: bytes, addr: Tuple[str, int]):
                self._on_udp_data(data, _Address(*addr[0:2]))

            def error_received(_, e: OSError):
                logging.warning(f"UDP transport error: {e}")

        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: Protocol(),
            local_addr=("0.0.0.0", self.port),
            allow_broadcast=True)

        # Regularly broadcast announcements
        while True:
            logging.debug("Sending peer announcement...")
            transport.sendto(self.announcement, ("<broadcast>", self.port))
            await asyncio.sleep(self.announce_interval)

    # On receiving announcement, add peer address to cache with an expiry timer
    # If it is already in our cache, reset its expiry timer
    def _on_udp_data(self, data: bytes, addr: _Address):

        # Ignore own announcements
        local_addrs = socket.getaddrinfo(socket.gethostname(), self.port)
        remote_addrs = socket.getaddrinfo(socket.getfqdn(addr.host), addr.port)
        for (_, _, _, _, local_addr) in local_addrs:
            for (_, _, _, _, remote_addr) in remote_addrs:
                if remote_addr == local_addr:
                    return

        # Ignore malformed announcements
        if data != self.announcement:
            return

        # Cancel previous expiry timer
        logging.debug(f"Received peer announcement from {addr}.")
        if addr in self.peers:
            self.peers[addr].cancel()
        else:
            logging.info(f"Added new peer: {addr}.")

        # Insert new peer address and expiry timer
        async def _do_peer_timeout():
            await asyncio.sleep(self.peer_timeout)
            logging.info(f"Removed timed-out peer: {addr}.")
            del self.peers[addr]
        self.peers[addr] = asyncio.create_task(_do_peer_timeout())

    # Start listening for connecting peers
    async def _start_tcp(self):
        logging.debug("Creating TCP server...")
        server = await asyncio.start_server(self._on_tcp_conn, None, self.port)
        await server.serve_forever()

    # Handle peer connection
    async def _on_tcp_conn(self, reader: StreamReader, writer: StreamWriter):
        addr = _Address(*writer.get_extra_info("sockname")[0:2])
        logging.debug(f"Handling TCP connection from {addr}...")

        # Read entire message
        msg_bytes = await reader.read()
        writer.close()
        msg = json.loads(msg_bytes)

        # Ignore invalid messages
        if msg["version"] != VERSION or msg["type"] != "set":
            return

        # Extract name and data
        name = msg["name"]
        data = {"value": msg["value"], "time": msg["time"]}

        # Ignore duplicate messages
        if name in self.dataset and data["time"] <= self.dataset[name]["time"]:
            logging.debug(f"Ignoring duplicate {name} from {addr}: {data}")
            return

        # Insert new data into data and forward to other peers
        logging.info(f"Received update to data {name} from {addr}: {data}")
        self.dataset[name] = data
        other_addrs = [peer for peer in self.peers if peer != addr]
        await self._send_to_addrs(msg_bytes, other_addrs)

    # Send bytes over TCP to list of addresses
    async def _send_to_addrs(self, msg_bytes: bytes, addrs: List[_Address]):

        # Send bytes to a single address
        async def send(addr):
            logging.debug(f"Sending message to {addr}...")
            _, writer = await asyncio.open_connection(addr.host, addr.port)
            writer.write(msg_bytes)
            writer.close()

        # Attempt to send bytes asynchronously to every address
        aws = [asyncio.create_task(send(addr)) for addr in addrs]
        if len(aws) != 0:
            _, pending = await asyncio.wait(aws, timeout=10)
            if len(pending) != 0:
                [task.cancel() for task in pending]
                await asyncio.wait(pending)

    # Construct well-formatted "announcement" message bytes
    def _create_announcement_msg(self):
        return json.dumps({
            "version": VERSION,
            "type": "announcement"
        }).encode()

    # Construct well-formatted "set" message bytes for data
    def _create_set_msg(self, name: str):
        return json.dumps({
            "version": VERSION,
            "type": "set",
            "name": name,
            "value": self.dataset[name]["value"],
            "time": self.dataset[name]["time"]
        }).encode()
