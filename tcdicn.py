import asyncio
import json
import logging
import signal
import socket
import time
from asyncio import StreamWriter, StreamReader
from asyncio import DatagramTransport, DatagramProtocol
from typing import List, Tuple, Dict


# The version of this protocol implementation is included in all communications
# This allows peers which implement one or more versions to react appropriately
VERSION: str = "0.2-dev"


# Every peer of every node is given a score to get to each known client
# For now, this is simply 1000 - #hops, but could be improved to take
# network congestion along the route into account
# TODO(score): congestion penalty
_Score = float


# The name of named data
_Tag = str


# The data of named data
class _TagInfo:
    def __init__(self, value: str, new_time: float):
        self.value = value  # The actual data
        self.new_time = new_time  # When the data was published


# Every node maintains a table of known interests for every known client
# These interests only become known if the node is on the shortest path
# between the subscriber client and one of the publishers of the interest
class _InterestInfo:
    def __init__(self, eol: float, last_time: float):
        self.eol = eol  # End Of Life: When the interest will expire
        self.last_time = last_time  # Interest only in data fresher than this


# Every client is identified by a universally unique string
# Clashes are not fatal but will result in both nodes having the interests or
# the interest data of the other node being spread towards it by the network
_ClientId = str


# Every node maintains a table of known clients
# This information is gossiped via the UDP broadcasts between peers
class _ClientInfo:
    def __init__(self, ttp: float, eol: float, tags: List[_Tag]):
        self.timer = None  # A task to expire this entry
        self.ttp = ttp  # Time To Propagate: Max time node can batch broadcast
        self.eol = eol  # End Of Life: When the client will expire
        self.tags = tags  # Tags this client is known to publish
        self.interests: Dict[_Tag, _InterestInfo] = dict()  # See _InterestInfo


# Client adverts, contained within a peer UDP advertisement, let other nodes
# know that a client can be reached via the source of the peer advertisement
# Adverts include a score so that nodes can pick the best peer to a client
class _ClientAdvert:
    def __init__(self, info: _ClientInfo, score: _Score):
        self.info = info
        self.score = score


# Peers of a node are identified by their host and port
# Provides a couple methods for easier comparison and printing
class _PeerId:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def __eq__(self, other) -> bool:
        return self.host == other.host and self.port == other.port

    def __hash__(self) -> int:
        return hash((self.host, self.port))

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


# Every node maintains a table of known peers
# Nodes advertise their presence to their peers with UDP broadcasts
class _PeerInfo:
    def __init__(self, eol: float):
        self.timer = None  # A task to expire this entry
        self.eol = eol  # End Of Life: When this peer will expire
        self.routes: Dict[_ClientId, _Score] = dict()  # See _Score


# Utility function for setting up UDP transport and handling datagrams
# Necessary because asyncio does not provide the construct as it does with TCP
async def _start_udp_transport(callback, host: str, port: int) \
        -> Tuple[DatagramTransport, DatagramProtocol]:

    class Protocol:

        def connection_made(_, transport: DatagramTransport):
            pass

        def connection_lost(_, e: Exception):
            logging.warning(f"UDP transport lost: {e}")

        def datagram_received(_, msg_bytes: bytes, src: Tuple[str, int]):
            # Ignore nodes own broadcast messages
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


# Send a UDP broadcast advertisement to all peers
# Adverts let other servers know we exist and should be remembered until eol
# We should also include a list of clients we know about so other servers can
# push us interests/data to get to the destination client, see _ClientAdvert
async def _send_advert_msg(
        peer: _PeerId, udp: DatagramTransport,
        eol: float, clients: Dict[_ClientId, _ClientAdvert]):
    udp.sendto(json.dumps({
        "version": VERSION,
        "type": "advert",
        "eol": eol,
        "clients": {
            client: {
                "ttp": advert.info.ttp,
                "eol": advert.info.eol,
                "tags": advert.info.tags,
                "score": advert.score
            } for client, advert in clients.items()
        },
    }).encode(), (peer.host, peer.port))


# Push an interest for tags that are fresher than a time to some peer address
# The interest belongs to client id and will expire at time eol
# This should be pushed all the way towards all relevant publishers along
# the shortest route as defined by the _ClientInfo.tags and _PeerInfo.routes
async def _send_get_msg(
        peer: _PeerId, ttp: float, eol: float,
        tag: _Tag, last_time: float, id: _ClientId):
    _, writer = await asyncio.open_connection(peer.host, peer.port)
    writer.write(json.dumps({
        "version": VERSION,
        "type": "get",
        "ttp": ttp,
        "eol": eol,
        "tag": tag,
        "time": last_time,
        "client": id
    }).encode())
    await writer.drain()
    writer.close()


# Push published value for tag with a given time to some peer address
# This should be pushed back towards all relevant subscribers along the
# shortest routes as defined by the _ClientInfo.interests and _PeerInfo.routes
async def _send_set_msg(peer: _PeerId, tag: _Tag, value: str, new_time: float):
    _, writer = await asyncio.open_connection(peer.host, peer.port)
    writer.write(json.dumps({
        "version": VERSION,
        "type": "set",
        "tag": tag,
        "value": value,
        "time": new_time
    }).encode())
    await writer.drain()
    writer.close()


# Provides all the networking logic for interacting with a network of ICN nodes
# It is required to be the only server running on the PI as it must listen on
# 33333 to implement discovery+advertising to other ICN nodes on the network
class Server:

    # Starts the server listening on a given port with a given peer broadcast
    # Time To Live (TTL) and TTL PreFire (TPF) factor
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

    # Start listening for UDP broadcast adverts and regularly broadcast our own
    async def _start_udp(self):
        logging.debug("Creating UDP server...")
        udp, _ = await _start_udp_transport(self._on_udp_data, None, self.port)
        while True:
            logging.debug("Broadcasting advertisement...")

            # Construct a table of clients to advertise to our peers
            # TODO(optimisation): split up message to avoid fragmentation
            # TODO(optimisation): respect clients ttp by broadcasting earlier
            clients: Dict[_ClientId, _ClientAdvert] = dict()
            for client, info in self.clients.items():
                if info.ttp is None:
                    continue  # Only broadcast clients we have just heard of
                # TODO(optimisation): this should be done elsewhere
                peer = self._get_best_peer_to_client(client)
                max_score = self.peers[peer].routes[client]
                clients[client] = _ClientAdvert(info, max_score - 1)
                info.ttp = None

            # Broadcast our advertisement
            addr = _PeerId("<broadcast>", self.port)
            eol = time.time() + self.net_ttl
            try:
                await _send_advert_msg(addr, udp, eol, clients)
            except OSError as e:
                logging.error(f"Error broadcasting advert: {e}")

            # Repeat a number of times before our TTL can run out
            await asyncio.sleep(self.net_ttl / self.net_tpf)

    # Handle UDP advertisement from peers
    def _on_udp_data(self, msg_bytes: bytes, peer: _PeerId):
        logging.debug(f"Handling UDP datagram from {peer}...")

        # Parse advertisement
        msg = json.loads(msg_bytes)
        if msg["version"] != VERSION and msg["type"] != "advert":
            logging.warning(f"Received bad datagram from {peer}; ignoring.")
            return
        eol = msg["eol"]
        clients: Dict[_ClientId, _ClientAdvert] = dict()
        for client, advert in msg["clients"].items():
            info = _ClientInfo(advert["ttp"], advert["eol"], advert["tags"])
            clients[client] = _ClientAdvert(info, advert["score"])

        # Update clients
        for client, advert in clients.items():
            if client in self.clients \
                    and advert.info.eol <= self.clients[client].eol:
                continue  # Ignore old client adverts caused by loops
            self._update_client(client, advert.info)

        # Update peer even if eol is smaller because loops cannot occur
        self._update_peer(peer, eol)

        # Update routes to client via peer scores
        for client, advert in clients.items():
            self.peers[peer].routes[client] = advert.score
            logging.debug(f"Set {client} via {peer} score: {advert.score}")

    # Process a new client advertisement from a peer UDP broadcast
    def _update_client(self, client: _ClientId, info: _ClientInfo):

        # Cancel previous client expiry timer
        if client in self.clients:
            self.clients[client].timer.cancel()
            self.clients[client].ttp = info.ttp
            self.clients[client].eol = info.eol
            self.clients[client].tags = info.tags
            logging.debug(f"Refreshed client: {client}")
        else:
            self.clients[client] = info
            logging.info(f"Added new client: {client}")

        # Insert client into clients table
        async def _do_timeout():
            await asyncio.sleep(self.clients[client].eol - time.time())
            del self.clients[client]
            logging.info(f"Removed client: {client}")
        self.clients[client].timer = asyncio.create_task(_do_timeout())

    # Process a peer advertisement from a peer UDP broadcast
    def _update_peer(self, peer: _PeerId, eol: float):

        # Cancel previous peer expiry timer
        if peer in self.peers:
            self.peers[peer].timer.cancel()
            self.peers[peer].eol = eol
            logging.debug(f"Refreshed peer: {peer}")
        else:
            self.peers[peer] = _PeerInfo(eol)
            logging.info(f"Added new peer: {peer}")

        # Insert peer into peers table
        async def _do_timeout():
            await asyncio.sleep(self.peers[peer].eol - time.time())
            del self.peers[peer]
            logging.info(f"Removed peer: {peer}")
        self.peers[peer].timer = asyncio.create_task(_do_timeout())

    # Start listening for connecting peers
    async def _start_tcp(self):
        logging.debug("Creating TCP server...")
        server = await asyncio.start_server(self._on_tcp_conn, None, self.port)
        await server.serve_forever()

    # Handle a peer connection
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
            await self._process_get_msg(peer, msg)
        elif msg["type"] == "set":
            await self._process_set_msg(peer, msg)

        """
        # TODO(optimisation): incorporate ttp into batching responses
        # TODO(optimisation): precompute and memorise a lot of this
        # For now, immediately respond if we can
        for client, client_info in self.clients.items():
            for interest, interest_info in client_info.interests.items():
                for tag, tag_info in self.content.items():
                    if interest == tag and tag_info.time > interest_info.time:
                        # TODO: handle failure
                        logging.debug(f"Pushing set {tag} towards {client}")
                        peer = self._get_best_peer_to_client(client)
                        if peer:
                            try:
                                await _send_set_msg(
                                    peer, tag, tag_info.value, tag_info.time)
                            except OSError as e:
                                logging.error(f"Error publishing data: {e}")
                        else:
                            logging.warning("Unable to do the thing 2")
        """

    async def _process_get_msg(self, peer: _PeerId, msg):
        ttp = msg["ttp"]  # TODO(optimisation): batching responses
        eol = msg["eol"]
        tag = msg["tag"]
        last_time = msg["time"]
        client = msg["client"]
        logging.debug(f"Received get from {peer}: {tag}>{last_time} @{client}")

        # We don't (yet) know this client, so create a placeholder for now
        if client not in self.clients:
            logging.warning(f"Received get from {peer} for unknown {client}")
            self.clients[client] = _ClientInfo(None, eol, [])

        # Update interest if newer time or later eol is received
        if tag not in self.clients[client].interests \
                or last_time > self.clients[client].interests[tag].last_time \
                or (last_time == self.clients[client].interests[tag].last_time
                    and eol > self.clients[client].interests[tag].eol):
            self.clients[client].interests[tag] = _InterestInfo(eol, last_time)
            """
            # TODO: eol timer tasks
            # TODO: push new gets towards publishers if eol is greater than
            # current max. For now, always push it
            for client, client_info in self.clients.items():
                if tag in client_info.tags:
                    logging.debug(f"Pushing get {tag} towards {client}")
                    peer = self._get_best_peer_to_client(client)
                    if peer:
                        try:
                            await _send_get_msg(
                                peer, ttp, eol, tag, last_time, client)
                        except OSError as e:
                            logging.error(f"Error sending interest: {e}")
                    else:
                        logging.warning("Unable to do the thing")
            """

    async def _process_set_msg(self, peer: _PeerId, msg):
        tag = msg["tag"]
        value = msg["value"]
        new_time = msg["time"]
        logging.debug(f"Received set from {peer}: {tag}={value}@{new_time}")

        if tag in self.content and self.content[tag].new_time >= new_time:
            return  # Ignore old publishes

        logging.info(f"Received update from {peer}: {tag}={value}@{new_time}")
        self.content[tag] = _TagInfo(value, new_time)

        # TODO(v0.2): remove gossiping once routing is finished
        for peer in self.peers:
            try:
                await _send_set_msg(peer, tag, value, new_time)
            except OSError as e:
                logging.error(f"Error publishing value: {e}")

    # Compute the best peer to go via to get to client based on known scores
    def _get_best_peer_to_client(self, client: _ClientId) -> _PeerId:
        best_score = 0
        best_peer = None
        for peer, peer_info in self.peers.items():
            for peer_client, score in peer_info.routes.items():
                if peer_client == client and score > best_score:
                    best_score = score
                    best_peer = peer
        return best_peer


# Provides all the networking logic for interacting with a single ICN node
# This allows you to run multiple sensors and actuators as additional processes
# on different ports which communicate with the local ICN node running on 33333
class Client:

    # Starts the client listening on a given port with a given peer broadcast
    # Time To Live (TTL), TTL Prefire (TPF) factor and Time To Propagate (TTP),
    # as well as a list of tags to advertise as being published by this node
    # and the address of the local ICN server to communicate with
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
        self.content: Dict[_Tag, _TagInfo] = dict()

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

    # Subscribes to tag and returns first new value received
    # Repeats request every TTL/TPF seconds until successful or cancelled
    # Allows each intermediate node to batch responses for up to TTP seconds
    async def get(self, tag: _Tag, ttl: float, tpf: int, ttp: float) -> bytes:

        # Many get() calls can be waiting on one pending interests
        if tag not in self.pending_interests:
            loop = asyncio.get_running_loop()
            self.pending_interests[tag] = loop.create_future()
            logging.debug(f"Added new local interest for {tag}.")

        # Subscribe to any data with a freshness greater than the last
        last_time = self.content[tag].new_time if tag in self.content else 0

        # Keep trying until either success or this coroutine is cancelled
        async def subscribe():
            while not self.pending_interests[tag].done():
                logging.debug(f"Sending new interest for {tag}...")
                try:
                    await _send_get_msg(
                        self.server, ttp, time.time() + ttl,
                        tag, last_time, self.id)
                except OSError as e:
                    logging.error(f"Error sending interest: {e}")
                await asyncio.sleep(ttl / tpf)
        task = asyncio.create_task(subscribe())
        value = await self.pending_interests[tag]
        task.cancel()
        return value

    # Publishes a new value to a tag
    # This will only be propagated towards interested clients
    async def set(self, tag: str, value: str):
        try:
            await _send_set_msg(self.server, tag, value, time.time())
        except OSError as e:
            logging.error(f"Error publishing value: {e}")

    # Start regularly sending UDP advertisements to the local ICN server to
    # let the rest of the network know this client exists
    async def _start_udp(self):
        logging.debug("Creating UDP server...")
        udp, _ = await _start_udp_transport(self._on_udp_data, None, self.port)
        while True:
            logging.debug("Sending advertisement to server...")
            eol = time.time() + self.net_ttl
            info = _ClientInfo(self.net_ttp, eol, self.tags)
            advert = _ClientAdvert(info, 1000)
            clients = {self.id: advert}
            try:
                await _send_advert_msg(self.server, udp, eol, clients)
            except OSError as e:
                logging.error(f"Error broadcasting advert: {e}")
            await asyncio.sleep(self.net_ttl / self.net_tpf)

    # Clients should not receive any UDP advertisement as they should not be
    # listening on the standard port
    def _on_udp_data(self, msg_bytes: bytes, peer: _PeerId):
        logging.warning(f"Received unexpected datagram from {peer}; ignoring.")

    # Start listening for connections from the ICN server
    async def _start_tcp(self):
        logging.debug("Creating TCP server...")
        server = await asyncio.start_server(self._on_tcp_conn, None, self.port)
        await server.serve_forever()

    # Handle a connection from the ICN server
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
        new_time = msg["time"]

        # Fulfill associated pending interest
        if tag in self.pending_interests:
            self.content[tag] = _TagInfo(value, new_time)
            self.pending_interests[tag].set_result(value)
            del self.pending_interests[tag]
            logging.info(f"Fulfilled local interest in {tag} @ {new_time}")
