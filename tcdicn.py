import asyncio
import json
import logging
import signal
import socket
import time
from asyncio import DatagramTransport, StreamWriter, StreamReader
from typing import List, Tuple, Dict


VERSION = "0.2-dev"


# TODO(comment)
_Tag = str


# TODO(comment)
# TODO(bug): should also include a uuid
class _TagInfo:
    def __init__(self, value: str, time: float):
        self.value = value
        self.time = time


class _InterestInfo:
    def __init__(self, eol: float, time: float):
        self.eol = eol
        self.time = time


# TODO(comment)
_ClientId = str


# TODO(comment)
class _ClientInfo:
    def __init__(self):
        self.timer = None
        self.ttp = None
        self.eol = None
        self.tags = list()
        self.interests = dict()


# Utility class for storing the address of peers in a useful structure
class _PeerId:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def __eq__(self, other):
        return self.host == other.host and self.port == other.port

    def __hash__(self):
        return hash((self.host, self.port))

    def __str__(self):
        return f"{self.host}:{self.port}"


# TODO(comment)
class _PeerInfo:
    def __init__(self):
        self.timer = None
        self.eol = None
        self.routes = dict()


# Utility function for setting up UDP transport and handling datagrams
async def _start_udp_transport(callback, host: str, port: int):
    class Protocol:

        def connection_made(_, transport: DatagramTransport):
            logging.debug(f"UDP transport established: {transport}")

        def connection_lost(_, e: Exception):
            logging.warning(f"UDP transport lost: {e}")

        def datagram_received(_, msg_bytes: bytes, src: Tuple[str, int]):
            # Ignore own messages
            l_addrs = socket.getaddrinfo(socket.gethostname(), port)
            r_addrs = socket.getaddrinfo(socket.getfqdn(src[0]), src[1])
            for (_, _, _, _, l_addr) in l_addrs:
                for (_, _, _, _, r_addr) in r_addrs:
                    if r_addr == l_addr:
                        return
            callback(msg_bytes, _PeerId(src[0], src[1]))

        def error_received(_, e: OSError):
            logging.warning(f"UDP transport error: {e}")

    return await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: Protocol(),
        local_addr=(host if host is not None else "0.0.0.0", port),
        allow_broadcast=True)


# Provides all the networking logic for interacting with a network of ICN nodes
# It is required to be the only server running on the PI as it must listen on
# 33333 to implement discovery+advertising to other ICN nodes on the network
class Server:

    # TODO(comment)
    def __init__(self, port: int, net_ttl: float, net_tpf: int):
        self.port = port
        self.net_ttl = net_ttl
        self.net_tpf = net_tpf

        # Initialise state
        # TODO(optimisation): load from disk in case of reboot
        self.content: Dict[_Tag, _TagInfo] = dict()
        self.clients: Dict[_ClientId, _ClientInfo] = dict()
        self.peers: Dict[_PeerId, _PeerInfo] = dict()

        # Start UDP and TCP servers
        udp_task = asyncio.create_task(self._start_udp())
        tcp_task = asyncio.create_task(self._start_tcp())
        self.task = asyncio.gather(udp_task, tcp_task)
        logging.info(f"Listening on :{self.port}")

        # Shutdown if we receive a signal
        loop = asyncio.get_running_loop()
        sigs = [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]
        [loop.add_signal_handler(s, lambda: self.task.cancel()) for s in sigs]

    # TODO(comment)
    async def _start_udp(self):
        logging.debug("Creating UDP server...")
        udp, _ = await _start_udp_transport(self._on_udp_data, None, self.port)
        while True:
            logging.debug("Broadcasting advertisement...")
            # TODO(optimisation): split up message to avoid fragmentation
            msg_bytes = self._create_advertisement_msg()
            udp.sendto(msg_bytes, ("<broadcast>", self.port))
            await asyncio.sleep(self.net_ttl / self.net_tpf)

    # TODO(comment)
    def _on_udp_data(self, msg_bytes: bytes, peer: _PeerId):
        logging.debug(f"Handling UDP datagram from {peer}...")

        # Parse announcement
        msg = json.loads(msg_bytes)
        if msg["version"] != VERSION and msg["type"] != "announcement":
            logging.warning(f"Received bad datagram from {peer}; ignoring.")
            return
        eol = msg["eol"]
        clients = dict()
        for client, info in msg["clients"].items():
            clients[client] = _ClientInfo()
            clients[client].ttp = msg["ttp"]
            clients[client].eol = msg["eol"]
            clients[client].tags = msg["tags"]

        # Update clients
        for client, info in clients.items():
            if client in self.clients and info.eol <= self.clients[client].eol:
                continue  # Ignore old client information due to loops
            self._update_client(client, info)

        # Update peer even if eol is smaller because loops cannot occur
        self._update_peer(peer, eol)

        # Update routes to client via peer scores
        for client, info in clients.items():
            self.peers[peer].routes[client] = info["score"]
            logging.debug(f"Set {client} via {peer} score: {info['score']}")

    # TODO(comment)
    def _update_client(self, client: _ClientId, info: _ClientInfo):

        # Cancel previous client expiry timer
        if client in self.clients:
            self.clients[client].timer.cancel()
            logging.debug(f"Refreshed client: {client}")
        else:
            self.clients[client] = _ClientInfo()
            logging.info(f"Added new client: {client}")

        # Update client
        self.clients[client].ttp = info.ttp
        self.clients[client].eol = info.eol
        self.clients[client].tags = info.tags
        logging.info(f"Set {client} tags: {info.tags}")

        # Insert client into client cache
        async def _do_timeout():
            await asyncio.sleep(self.clients[client].eol - time.time())
            del self.clients[client]
            logging.info(f"Removed client: {client}")
        self.clients[client].timer = asyncio.create_task(_do_timeout())

        # TODO(optimisation): incorporate ttp into avoiding udp fragmentation

    # TODO(comment)
    def _update_peer(self, peer: _PeerId, eol: int):

        # Cancel previous peer expiry timer
        if peer in self.peers:
            self.peers[peer].timer.cancel()
            logging.debug(f"Refreshed peer: {peer}")
        else:
            self.peers[peer] = _PeerInfo()
            logging.info(f"Added new peer: {peer}")

        # Update peer
        self.peers[peer].eol = eol

        # Insert peer into peer cache
        async def _do_timeout():
            await asyncio.sleep(self.peers[peer].eol - time.time())
            del self.peers[peer]
            logging.info(f"Removed peer: {peer}")
        self.peers[peer].timer = asyncio.create_task(_do_timeout())

    # TODO(comment)
    def _create_advertisement_msg(self):
        clients = dict()
        for client, info in self.clients.items():
            if info.ttp is not None:
                # TODO(clean): this should be done elsewhere
                all_routes = [self.peers[peer].routes for peer in self.peers]
                routes = [routes for routes in all_routes if client in routes]
                routes = [routes[client] for routes in routes]
                max_score = max([score for score in routes])
                max_score /= 2  # TODO(score): apply congestion penalty
                clients[client] = dict()
                clients[client]["ttp"] = info.ttp
                clients[client]["eol"] = info.eol
                clients[client]["score"] = max_score
                clients[client]["tags"] = info.tags
                info.ttp = None
        return json.dumps({
            "version": VERSION,
            "type": "announcement",
            "eol": time.time() + self.net_ttl,
            "clients": clients
        }).encode()

    # Start listening for connecting peers
    async def _start_tcp(self):
        logging.debug("Creating TCP server...")
        server = await asyncio.start_server(self._on_tcp_conn, None, self.port)
        await server.serve_forever()

    # Handle peer connection
    async def _on_tcp_conn(self, reader: StreamReader, writer: StreamWriter):
        peer = _PeerId(*writer.get_extra_info("peername")[0:2])
        logging.debug(f"Handling TCP connection from {peer}...")

        # Read entire message
        msg_bytes = await reader.read()
        writer.close()
        msg = json.loads(msg_bytes)

        # Parse message
        if msg["version"] != VERSION or msg["type"] not in ["get", "set"]:
            logging.warning(f"Received bad message from {peer}; ignoring.")
            return

        if msg["type"] == "get":
            # ttp = msg["ttp"]  # TODO(optimisation): batching responses
            eol = msg["eol"]
            tag = msg["tag"]
            time = msg["time"]
            client = msg["client"]
            logging.debug(f"Received get from {peer}: {tag}>{time} ~ {client}")

            # We don't (yet) know this client, so create a placeholder for now
            if client not in self.clients:
                self.clients[client] = _ClientInfo()
                self.clients[client].ttp = None
                self.clients[client].eol = eol
                self.clients[client].tags = []

            # Update this interest
            if tag not in self.clients[client].interests or time > self.clients[client].interests[tag].time or (time == self.clients[client].interests[tag].time and eol > self.clients[client].interests[tag].eol):
                self.clients[client].interests[tag] = _InterestInfo(eol, time)
                # TODO: eol timer tasks

        elif msg["type"] == "set":
            tag = msg["tag"]
            value = msg["value"]
            time = msg["time"]
            logging.debug(f"Received set from {peer}: {tag}={value}@{time}")

            if tag in self.content and self.content[tag].time >= time:
                return  # Ignore old publishes

            logging.info(f"Received update from {peer}: {tag}={value}@{time}")
            self.content[tag] = _TagInfo(value, time)

            for client in self.clients:
                pass  # TODO: if interested

        # TODO(optimisation): incorporate ttp into batching responses
        # For now, immediately respond if we can
        for client, client_info in self.clients.items():
            for interest, interest_info in client_info.interests.items():
                for tag, tag_info in self.content.items():
                    if interest == tag and tag_info.time > interest_info.time:
                        pass
                        # TODO: respond to client


# Provides all the networking logic for interacting with a single ICN node
# This allows you to run multiple sensors and actuators as additional processes
# on different ports which communicate with the local ICN node running on 33333
class Client:

    # TODO(comment)
    def __init__(
            self, id: _ClientId, port: int, tags: List[_Tag],
            server_host: str, server_port: int,
            net_ttl: float, net_tpf: int, net_ttp: float):
        self.id = id
        self.port = port
        self.tags = tags
        self.server = _PeerId(server_host, server_port)
        self.net_ttl = net_ttl
        self.net_tpf = net_tpf
        self.net_ttp = net_ttp

        # Initialise state
        # TODO(optimisation): load from disk in case of reboot
        self.pending_interests: Dict[_Tag, asyncio.Future] = dict()
        self.history: Dict[_Tag, float] = dict()

        # Start UDP and TCP servers
        udp_task = asyncio.create_task(self._start_udp())
        tcp_task = asyncio.create_task(self._start_tcp())
        self.task = asyncio.gather(udp_task, tcp_task)
        logging.info(f"Pointed towards {self.server}")
        logging.info(f"Listening on :{self.port}")

        # Shutdown if we receive a signal
        loop = asyncio.get_running_loop()
        sigs = [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]
        [loop.add_signal_handler(s, lambda: self.task.cancel()) for s in sigs]

    # TODO(comment)
    async def get(self, tag: str, ttl: float, tpf: int, ttp: float):

        if tag not in self.pending_interests:
            loop = asyncio.get_running_loop()
            self.pending_interests[tag] = loop.create_future()
            logging.debug(f"Added new local interest for {tag}.")

        if tag not in self.history:
            self.history[tag] = 0
            logging.debug(f"Added new history for {tag}.")

        # TODO: resend every asyncio.sleep(ttl / tpf)

        _, writer = await asyncio.open_connection(
            self.server.host, self.server.port)
        writer.write(json.dumps({
            "version": VERSION,
            "type": "get",
            "ttp": ttp,
            "eol": time.time() + ttl,
            "tag": tag,
            "time": self.history[tag],
            "client": self.id
        }).encode())
        await writer.drain()
        writer.close()

        logging.debug(f"Waiting on pending interest for {tag}...")
        value = await self.pending_interests[tag]
        return value

    # TODO(comment)
    async def set(self, tag: str, value: str):
        _, writer = await asyncio.open_connection(
            self.server.host, self.server.port)
        writer.write(json.dumps({
            "version": VERSION,
            "type": "set",
            "tag": tag,
            "value": value,
            "time": time.time()
        }).encode())
        await writer.drain()
        writer.close()

    # TODO(comment)
    async def _start_udp(self):
        logging.debug("Creating UDP server...")
        udp, _ = await _start_udp_transport(self._on_udp_data, None, self.port)
        while True:
            logging.debug("Sending advertisement to server...")
            msg_bytes = self._create_advertisement_msg()
            udp.sendto(msg_bytes, (self.server.host, self.server.port))
            await asyncio.sleep(self.net_ttl / self.net_tpf)

    # TODO(comment)
    def _on_udp_data(self, msg_bytes: bytes, peer: _PeerId):
        logging.warning(f"Received unexpected datagram from {peer}; ignoring.")

    # TODO(comment)
    def _create_advertisement_msg(self):
        eol = time.time() + self.net_ttl
        return json.dumps({
            "version": VERSION,
            "type": "announcement",
            "eol": eol,
            "clients": {
                self.id: {
                    "ttp": self.net_ttp,
                    "eol": eol,
                    "score": 100,  # TODO(score): arbitrary
                    "tags": self.tags,
                }
            }
        }).encode()

    # TODO(comment)
    async def _start_tcp(self):
        logging.debug("Creating TCP server...")
        server = await asyncio.start_server(self._on_tcp_conn, None, self.port)
        await server.serve_forever()

    # TODO(comment)
    async def _on_tcp_conn(self, reader: StreamReader, writer: StreamWriter):
        peer = _PeerId(*writer.get_extra_info("peername")[0:2])
        logging.debug(f"Handling TCP connection from {peer}...")

        # Read entire message
        msg_bytes = await reader.read()
        writer.close()
        msg = json.loads(msg_bytes)

        # Parse set message
        if msg["version"] != VERSION or msg["type"] != "set":
            logging.warning(f"Received bad message from {peer}; ignoring.")
            return
        tag = msg["tag"]
        value = msg["value"]
        time = msg["time"]

        logging.debug(f"Received set from {peer}: {tag}={value} @ {time}")
        if tag in self.pending_interests:
            self.history[tag] = time  # TODO(optimisation): content store
            self.pending_interests[tag].set_future(value)
            del self.pending_interests[tag]
            logging.debug(f"Fulfilled local interest in {tag} @ {time}")
