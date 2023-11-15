import asyncio
import ipaddress
import json
import logging
import queue
import signal
import socket
import time
from abc import ABC, abstractmethod
from asyncio import Task, Future, DatagramTransport, StreamWriter, StreamReader
from logging import Logger, LoggerAdapter
from json import JSONDecodeError
from logging import Logger, LoggerAdapter
from typing import Dict, Tuple, List, Optional, TypeVar, Union

# The version of this protocol implementation is included in all communications
# This allows peers which implement one or more versions to react appropriately
VERSION: str = "0.2"

# The soft maximum size in bytes to allow batched client advert forwarding
# broadcasts, which limits the number of client adverts sent at once
# Generally a good idea set to less than the Maximum Transmission Unit (MTU)
# This minimises the chance of peer advert broadcasts being dropped/lost
BROADCAST_CAPACITY: int = 512

# Score clients should give themselves
MAX_SCORE = 10000

# Seconds to wait for a TCP connection to be established before giving up
TCP_TIMEOUT: float = 2

# Seconds to wait before retrying after exhausting all known routes to client
DEADLINE_EXT: float = 10

# Peers are identified solely by their host and port number
Addr = Tuple[str, int]


# Prepend logger output with some useful context
class ContextLogger(LoggerAdapter):
    def process(self, msg, kwargs):
        return f"{self.extra} | {msg}", kwargs


# Convert a UNIX timestamp into a human readable seconds since string
def to_human(timestamp: float) -> str:
    secs = timestamp - time.time()
    return f"in {secs} seconds" if secs >= 0 else f"{-secs} seconds ago"


# Execute callback after End Of Life timestamp - Useful for implementing caches
def do_after(eol: float, callback) -> Task:
    async def on_timeout():
        await asyncio.sleep(eol - time.time())
        callback()

    return asyncio.create_task(on_timeout())


# Nodes commutate using messages which can be sent over TCP or UDP
# A Message is only a JSON formatted version number and list of MessageItems
# JSON field names are shrunk to help pack more information into UDP datagrams

# This class is just define a common type between MessageItems
class MessageItem(ABC):
    @abstractmethod
    def to_dict(self) -> dict:
        ...

    @staticmethod
    @abstractmethod
    def from_dict(d: dict) -> "MessageItem":
        ...


# Lets other nodes know what time your End Of Life (EOL) is
# Without further PeerItems, you will be forgotten from the network after EOL
# As such, these should be broadcasted over UDP at regular intervals before EOL
class PeerItem(MessageItem):
    def __init__(self, eol: float):
        self.eol = eol
        self.timer: Optional[Task] = None  # Used internal within nodes to timeout entry

    def to_dict(self) -> dict:
        return {
            "t": "p",
            "e": self.eol,
        }

    @staticmethod
    def from_dict(d: dict):
        if d["t"] != "p":
            raise ValueError("Not a peer message item")
        return PeerItem(d["e"])


# Tells other nodes about clients you know about and the route score
# Nodes that contain their own clients can include adverts for them in the same
# message as their regular PeerItem broadcast
# Time To Propagate (TTP) demands that nodes wait no more than TTP seconds
# before propagating this AdvertItem towards clients (due to batching reasons)
class AdvertItem(MessageItem):
    def __init__(
            self, client: str, labels: List[str],
            score: float, ttp: float, eol: float):
        self.client = client
        self.labels = labels
        self.score = score
        self.ttp = ttp
        self.eol = eol
        self.timer: Optional[Task] = None  # Used internal within nodes to timeout entry

    def to_dict(self) -> dict:
        return {
            "t": "a",
            "c": self.client,
            "l": self.labels,
            "s": self.score,
            "p": self.ttp,
            "e": self.eol,
        }

    @staticmethod
    def from_dict(d: dict):
        if d["t"] != "a":
            raise ValueError("Not an advert message item")
        return AdvertItem(d["c"], d["l"], d["s"], d["p"], d["e"])


# An expression of interest in data of some label published after some time
# Is pushed towards known clients who have listed the label as one they publish
# Has an End Of Life (EOL) specifying when the interest should be forgotten
# Time To Propagate (TTP) demands that nodes wait no more than TTP seconds
# before propagating this GetItem towards publishers (due to batching reasons)
class GetItem(MessageItem):
    def __init__(
            self, client: str, label: str,
            after: float, ttp: float, eol: float):
        self.client = client
        self.label = label
        self.after = after
        self.ttp = ttp
        self.eol = eol
        self.timer = None  # Used internal within nodes to timeout entry

    def to_dict(self) -> dict:
        return {
            "t": "g",
            "c": self.client,
            "l": self.label,
            "a": self.after,
            "p": self.ttp,
            "e": self.eol,
        }

    @staticmethod
    def from_dict(d: dict):
        if d["t"] != "g":
            raise ValueError("Not a get request message item")
        return GetItem(d["c"], d["l"], d["a"], d["p"], d["e"])


# Request to cache and propagate the contained data towards interested clients
# Time To Propagate (TTP) demands that nodes wait no more than TTP seconds
# before propagating this SetItem towards subscribers (due to batching reasons)
class SetItem(MessageItem):
    def __init__(
            self, label: str, data: Optional[str],
            at: float, dst: List[Tuple[float, str]]):
        self.label = label
        self.data = data
        self.at = at
        self.dst = dst
        # Used internal within nodes to allow .get() to always return new data
        self.last: float = 0
        self.fulfil: Optional[Future] = None

    def to_dict(self) -> dict:
        return {
            "t": "s",
            "l": self.label,
            "d": self.data,
            "a": self.at,
            "c": self.dst,
        }

    def from_dict(d: dict):
        if d["t"] != "s":
            raise ValueError("Not a set request message item")
        return SetItem(d["l"], d["d"], d["a"], d["c"])


# The data structure passed between nodes on the network in JSON format
class Message:
    def __init__(self, items: List[MessageItem]):
        self.version = VERSION
        self.items = items

    def to_dict(self) -> dict:
        return {
            "v": VERSION,
            "i": [item.to_dict() for item in self.items]
        }

    def from_dict(d: dict):
        if d["v"] != VERSION:
            raise ValueError("Message version unsupported:", d["v"])
        t_map = {"p": PeerItem, "a": AdvertItem, "g": GetItem, "s": SetItem}
        return Message([t_map[item["t"]].from_dict(item) for item in d["i"]])

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode()

    def from_bytes(data: bytes):
        return Message.from_dict(json.loads(data))


# Provides all the networking logic for interacting with a network of ICN nodes
# While many can be listening on many ports on the PI at once, one must serve
# as the PI master node listening on the default port (33333, which should be
# provided to all nodes as the dport parameter) so that node discovery can work
# For a node to be a client (something that either subscribes to or publishes
# data to the network), you must provide a ClientInfo to start() which contains
# a network-wide unique name as its network identifier
# Duplicate names are not fatal but significantly reduce the networks ability
# to send interests and data to only places that it is needed
class Node:
    def __init__(self):
        self.tcp = None
        self.advert = None
        self.dport = None
        self.port = None
        self.udp = None
        self.log = logging.getLogger(__name__)
        self.peers: Dict[Addr, PeerItem] = {}  # IP>Peer info
        self.clients: Dict[str, AdvertItem] = {}  # ID>Client info
        self.interests: Dict[str, Dict[str, GetItem]] = {}  # Label+ID>Interest
        self.routes: Dict[str, List[Dict]] = {}  # ID>Score+Route
        # TODO(optimisation): write to/read from disk
        self.content_store: Dict[str, SetItem] = {}  # Label >data

        self.batch_broadcast_task = None
        self.broadcast_queue = queue.PriorityQueue()
        self.is_broadcast_queue_changed = False

        self.batch_send_task = None
        self.send_queue = queue.PriorityQueue()
        self.is_send_queue_changed = False

    # Starts all tasks needed for the node to communicate with the network
    # Send the process a SIGINT or cancel the coroutine to shutdown the node
    async def start(
            self, port: int, dport: int,
            ttl: float, tpf: int,
            client: dict = None):
        self.port = port
        self.dport = dport
        self.is_main = (port == dport)

        self.advert = None if client is None else AdvertItem(
            client["name"], client["labels"], MAX_SCORE, client["ttp"], 0)

        loop = asyncio.get_running_loop()

        # Wrap UDP handling into self.on_datagram
        class UdpProtocol:
            def connection_made(_, _udp: DatagramTransport):
                self.log.debug("UDP transport established on :%s", self.port)

            def connection_lost(_, exc: Exception):
                self.log.warning("UDP transport closed, error: %s", exc)

            def datagram_received(_, msg: bytes, addr: Addr):
                self.on_datagram(msg, addr)

            def error_received(_, exc: OSError):
                self.log.warning("UDP transport error: %s", exc)

        # Start UDP and TCP server
        self.udp, _ = await loop.create_datagram_endpoint(
            lambda: UdpProtocol(),
            local_addr=("0.0.0.0", self.port),
            allow_broadcast=True)
        self.tcp = await asyncio.start_server(
            self.on_connection, "0.0.0.0", self.port)

        # Regularly broadcast own adverts TPF times before our TTL can run out
        async def do_regular_broadcasts():
            while True:
                try:
                    self.log.debug("Broadcasting to peers...")
                    items = [PeerItem(time.time() + ttl)]
                    if self.advert is not None:
                        self.advert.eol = items[0].eol
                        items.append(self.advert)
                    self.broadcast_msg(Message(items))
                except OSError as e:
                    self.log.warning("Error broadcasting: %s", e)
                await asyncio.sleep(ttl / tpf)

        # Run in background
        tcp_task = asyncio.create_task(self.tcp.serve_forever())
        reg_task = asyncio.create_task(do_regular_broadcasts())
        tasks = [tcp_task, reg_task]

        # Shutdown if we receive a signal
        def shutdown():
            self.log.info("Shutting down...")
            reg_task.cancel()
            self.udp.close()
            self.tcp.close()

        for sig in [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]:
            loop.add_signal_handler(sig, shutdown)

        # Wait until cancelled or shutdown
        try:
            async with self.tcp:
                self.log.info("Up and listening on :%s", self.port)
                self.log.info("Targeting :%s for discovery", self.dport)
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.exceptions.CancelledError:
            self.log.debug("Node tasks cancelled")
        self.log.info("Goodbye :)")

    # Subscribes to label and returns first new value received
    # Repeats request every TTL/TPF seconds until successful or cancelled
    # Allows each intermediate node to batch responses for up to TTP seconds
    async def get(self, label: str, ttl: float, tpf: int, ttp: float) -> str:
        log = ContextLogger(self.log, f"get {label}")
        if self.advert is None:
            raise RuntimeError("Only client nodes can subscribe")

        # Check if local content store already has a new value
        if label not in self.content_store:
            self.content_store[label] = SetItem(label, None, 0, [])
            log.debug("Created new label in local content store")
        if self.content_store[label].at > self.content_store[label].last:
            log.info("New value found in local content store")
        else:
            log.info("Subscribing for new values...")

            # Many get() calls can be waiting on one pending interests
            if self.content_store[label].fulfil is None \
                    or self.content_store[label].fulfil.done() \
                    or self.content_store[label].fulfil.cancelled():
                loop = asyncio.get_running_loop()
                self.content_store[label].fulfil = loop.create_future()
                log.debug("Created new local interest")

            # Keep trying until either success or this coroutine is cancelled
            async def subscribe():
                after = self.content_store[label].last
                while True:
                    log.debug("Sending get request...")
                    self.on_get(log, GetItem(
                        self.advert.client, label,
                        after, ttp, time.time() + ttl))
                    if self.is_send_queue_changed:
                        self.schedule_batch_send()
                        self.is_send_queue_changed = False
                    await asyncio.sleep(ttl / tpf)

            task = asyncio.create_task(subscribe())
            assert await self.content_store[label].fulfil
            task.cancel()

        self.content_store[label].last = self.content_store[label].at
        return self.content_store[label].data

    # Publishes a new value to a label
    # This will only be propagated towards interested clients
    async def set(self, label: str, data: str):
        log = ContextLogger(self.log, f"set {label}")
        log.info("Publishing...")
        dst = []
        if label in self.interests:
            for get_item in self.interests[label].values():
                dst.append((get_item.ttp, get_item.client))
        else:
            log.debug("Nobody is interested")
        self.on_set(log, SetItem(label, data, time.time(), dst))
        if self.is_send_queue_changed:
            self.schedule_batch_send()
            self.is_send_queue_changed = False

    # Batching

    def schedule_batch_send(self):

        # Check for previous scheduled batch
        if self.batch_send_task is not None:
            self.batch_send_task.cancel()
            self.batch_send_task = None

        # Find next item deadline
        try:
            deadline, client, routes, item = self.send_queue.get_nowait()
            self.send_queue.put_nowait((deadline, client, routes, item))
        except queue.Empty:
            return

        # Schedule new time
        now = time.time()
        eol = (deadline - now) / 2 + now
        task = do_after(eol, lambda: asyncio.create_task(self.batch_send()))
        self.batch_send_task = task
        self.log.debug("Scheduled next send batch: %s", to_human(eol))

    async def batch_send(self):
        log = ContextLogger(self.log, "tcp batch")

        accepted = []
        rejects = []
        addr = None

        # Include all items in queue destined to the next peer
        while True:
            try:
                deadline, client, routes, item = self.send_queue.get_nowait()
            except queue.Empty:
                break

            try:
                peer = routes[0]["addr"] if self.is_main \
                    else ("127.0.0.1", self.dport)  # Non-main push to main
            except IndexError:
                if client in self.routes:
                    routes = self.routes[client]
                rejects.append((deadline + DEADLINE_EXT, client, routes, item))
                log.warning("No route to %s", client)
                continue

            if addr is None:
                addr = peer
                log.debug("Batch destined to %s", addr)

            if peer == addr:
                accepted.append((deadline, client, routes, item))
                log.debug("Added %s", type(item).__name__)
            else:
                rejects.append((deadline, client, routes, item))
                log.debug("Rejected %s", type(item).__name__)

        # Put everything else back
        for reject in rejects:
            self.send_queue.put_nowait(reject)
        items = [item for _, _, _, item in accepted]

        # Send it!
        if addr is not None:
            try:
                await self.send_msg(addr, Message(items))
            except OSError:
                log.warning("Unable to contact %s", addr)
                ext = 0 if self.is_main else DEADLINE_EXT
                for deadline, client, routes, item in accepted:
                    self.send_queue.put_nowait(
                        (deadline + ext, client, routes[1:], item))
        else:
            log.warning("There was nothing to send")

        # Schedule next batch
        self.schedule_batch_send()

    def schedule_batch_broadcast(self):

        # Check for previous scheduled batch
        if self.batch_broadcast_task is not None:
            self.batch_broadcast_task.cancel()
            self.batch_broadcast_task = None

        # Find next item deadline
        try:
            deadline, item = self.broadcast_queue.get_nowait()
            self.broadcast_queue.put_nowait((deadline, item))
        except queue.Empty:
            return

        # Schedule new time
        now = time.time()
        eol = (deadline - now) / 2 + now
        task = do_after(eol, self.batch_broadcast)
        self.batch_broadcast_task = task
        self.log.debug("Scheduled next broadcast batch: %s", to_human(eol))

    def batch_broadcast(self):
        log = ContextLogger(self.log, "udp batch")

        items = []
        msg = Message([])
        msg_bytes = msg.to_bytes()
        msg_len = len(msg_bytes)

        # Add as many pending items to this broadcast as we safely can
        # Force at least one if this is any to prevent queue blocking
        while True:
            try:
                deadline, item = self.broadcast_queue.get_nowait()
            except queue.Empty:
                break

            # TODO(optimisation): score based on perceived congestion
            # for now, use some randomness to diversify routes taken
            import random
            if type(item) is AdvertItem:
                item.score -= 1 + random.uniform(0, 0.5)

            new_items = items + [item]
            new_msg = Message(new_items)
            new_msg_bytes = new_msg.to_bytes()
            new_msg_len = len(new_msg_bytes)
            diff = new_msg_len - msg_len
            if len(items) != 0 and new_msg_len >= BROADCAST_CAPACITY:
                log.debug("Refused %s (+%s bytes)", type(item).__name__, diff)
                self.broadcast_queue.put_nowait((deadline, item))
                break
            log.debug("Added %s (+%s bytes)", type(item).__name__, diff)

            items = new_items
            msg = new_msg
            msg_len = new_msg_len

        # Send it!
        try:
            self.broadcast_msg(msg)
        except OSError as e:
            log.warning("Error broadcasting batch: %s", e)

        # Schedule next batch
        self.schedule_batch_broadcast()

    # Network methods - May raise OSError

    async def send_msg(self, addr: Addr, msg: Message):
        msg_bytes = msg.to_bytes()
        connection = asyncio.open_connection(addr[0], addr[1])
        _, writer = await asyncio.wait_for(connection, timeout=TCP_TIMEOUT)
        writer.write(msg_bytes)
        await writer.drain()
        writer.close()
        self.log.debug(
            "Sent %s items: %s (%s bytes)",
            addr, len(msg.items), len(msg_bytes))

    # TODO(optimisation): use multicast instead of broadcast
    def broadcast_msg(self, msg: Message):
        msg_bytes = msg.to_bytes()
        host = socket.gethostname()
        addrs = socket.getaddrinfo(
            host, self.dport, family=socket.AF_INET, proto=socket.IPPROTO_UDP)
        for (_, _, _, _, (host, port)) in addrs:
            net = ipaddress.IPv4Network(host + "/24", False)  # NOTE: hardcoded
            self.udp.sendto(msg_bytes, (str(net.broadcast_address), port))
        self.log.debug(
            "Broadcasted items: %s (%s bytes)",
            len(msg.items), len(msg_bytes))

    # Network event handlers

    # UDP datagram entry point
    def on_datagram(self, data: bytes, addr: Addr):
        log = ContextLogger(self.log, f"UDP {addr[0]}:{addr[1]}")

        # Ignore own broadcasts
        l_addrs = socket.getaddrinfo(socket.gethostname(), self.port)
        r_addrs = socket.getaddrinfo(socket.getfqdn(addr[0]), addr[1])
        for (_, _, _, _, l_addr) in l_addrs:
            for (_, _, _, _, r_addr) in r_addrs:
                if r_addr == l_addr:
                    log.debug("Ignored broadcast from self")
                    return

        # Handle message
        self.on_message(log, addr, data)

    # TCP connection entry point
    async def on_connection(self, reader: StreamReader, writer: StreamWriter):
        addr = writer.get_extra_info("peername")[0:2]
        log = ContextLogger(self.log, f"TCP {addr[0]}:{addr[1]}")
        log.debug("New connection")

        # Read entire message
        try:
            data = await asyncio.wait_for(reader.read(), timeout=DATA_TIMEOUT)
        except asyncio.TimeoutError:
            log.warning("Read timed out")
            return
        except Exception as exc:
            log.warning("Error reading: %s", exc)
            return
        finally:
            writer.close()

        # Handle message
        self.on_message(log, addr, data)

    # Common logic for handling both TCP and UDP messages
    def on_message(self, log: Logger, addr: Addr, data: bytes):

        # Parse message
        try:
            msg = Message.from_bytes(data)
        except (JSONDecodeError, KeyError, ValueError):
            log.warning("Ignored malformed message")
            return
        if msg.version != VERSION:
            log.warning("Ignored message with version %s", msg.version)
            return

        # Handle message items appropriately
        for item in msg.items:
            if type(item) is PeerItem:
                self.on_peer(log, addr, item)
        for item in msg.items:
            if type(item) is AdvertItem:
                self.on_advert(log, addr, item)
        for item in msg.items:
            if type(item) is GetItem:
                self.on_get(log, item)
            elif type(item) is SetItem:
                self.on_set(log, item)

        # Reschedule next batches if necessary
        if self.is_broadcast_queue_changed:
            self.schedule_batch_broadcast()
            self.is_broadcast_queue_changed = False
        if self.is_send_queue_changed:
            self.schedule_batch_send()
            self.is_send_queue_changed = False

    # Handlers for each MessageItem type
    # If called directly, it is your responsibility to refresh any
    # scheduled tasks that should be affected (see on_message)

    def on_peer(self, log: Logger, addr: Addr, peer: PeerItem):
        log = ContextLogger(log, "peer")

        # Check for previous peer entry
        try:
            self.peers[addr].timer.cancel()
            log.debug("Refreshed peer")
        except KeyError:
            log.info("New peer")

        # Insert new peer entry with timeout
        def on_timeout():
            log.info("Timed out peer")
            del self.peers[addr]
            for client, entries in self.routes.items():
                for idx, route in enumerate(self.routes[client]):
                    if route["addr"] == addr:
                        del self.routes[client][idx]
                        break
        self.peers[addr] = peer
        self.peers[addr].timer = do_after(peer.eol, on_timeout)

    def on_advert(self, log: Logger, addr: Addr, advert: AdvertItem):
        log = ContextLogger(log, f"{advert.client}")
        if addr not in self.peers:
            log.warning("Received advert from unknown peer")
            self.on_peer(log, addr, PeerItem(advert.eol))
        if self.advert is not None and self.advert.client == advert.client:
            log.debug("Ignored advert for ourselves")
            return

        # Update routes to client via peer
        if advert.client not in self.routes:
            self.routes[advert.client] = []
        for idx, route in enumerate(self.routes[advert.client]):
            if route["addr"] == addr:
                self.routes[advert.client][idx]["score"] = advert.score
                break
        else:
            new_route = {"addr": addr, "score": advert.score}
            self.routes[advert.client].append(new_route)
        self.routes[advert.client].sort(
            key=lambda route: route["score"], reverse=True)

        # Check for previous client advert entry
        try:
            if advert.eol <= self.clients[advert.client].eol:
                log.debug("Ignored old advert")
                return
            self.clients[advert.client].timer.cancel()
            previous_labels = self.clients[advert.client].labels
            log.debug("Refreshed client")
        except KeyError:
            previous_labels = []
            log.info("New client")

        # Insert new entry with timeout
        def on_timeout():
            log.info("Timed out client")
            del self.clients[advert.client]
            del self.routes[advert.client]
        self.clients[advert.client] = advert
        self.clients[advert.client].timer = do_after(advert.eol, on_timeout)

        # Additions to listed published labels results in interest propagation
        for label in advert.labels:
            if label not in previous_labels and label in self.interests:
                for interest in self.interests[label].values():
                    deadline = time.time() + interest.ttp
                    routes = self.routes[advert.client] \
                        if advert.client in self.routes else []
                    self.send_queue.put_nowait(
                        (deadline, advert.client, routes, interest))
                    self.is_send_queue_changed = True
                    log.debug("New get deadline: %s", to_human(deadline))

        # Add advert to queue
        deadline = time.time() + advert.ttp
        self.broadcast_queue.put_nowait((deadline, advert))
        self.is_broadcast_queue_changed = True
        log.debug("New advert deadline: %s", to_human(deadline))

        # TODO(safely): cooldown / remove duplicates

    def on_get(self, log: Logger, g: GetItem):
        log = ContextLogger(log, f"get {g.label}>{g.after}@{g.client}")

        # Check for previous interest entry
        if g.label not in self.interests:
            log.info("New interest in label")
            self.interests[g.label] = {}
        try:
            if g.eol <= self.interests[g.label][g.client].eol:
                log.debug("Ignored old interest")
                return
            self.interests[g.label][g.client].timer.cancel()
            log.debug("Refreshed interest")
        except KeyError:
            log.info("New interest from client")

        # Insert new entry with timeout
        def on_timeout():
            log.info("Timed out interest")
            del self.interests[g.label][g.client]
            if len(self.interests[g.label]) == 0:
                log.info("No more interest for label")
                del self.interests[g.label]
        self.interests[g.label][g.client] = g
        self.interests[g.label][g.client].timer = do_after(g.eol, on_timeout)

        # Add gets towards known publishers to queue
        for client in self.clients:
            if g.label in self.clients[client].labels:
                if self.advert is None or self.advert.client != client:
                    deadline = time.time() + g.ttp
                    routes = self.routes[client] \
                        if client in self.routes else []
                    self.send_queue.put_nowait((deadline, client, routes, g))
                    self.is_send_queue_changed = True
                    log.debug("New get deadline: %s", to_human(deadline))

        # If we are a non-main node, we need to push to the device's main node
        if not self.is_main:
            deadline = time.time() + g.ttp
            self.send_queue.put_nowait((deadline, None, [], g))
            self.is_send_queue_changed = True
            log.debug("New main get deadline: %s", to_human(deadline))

        # If we can fulfil this get, add sets toward client to queue
        if g.label in self.content_store \
                and self.content_store[g.label].at > g.after:
            s = self.content_store[g.label]
            s.dst = [(g.ttp, g.client)]
            deadline = time.time() + g.ttp
            routes = self.routes[g.client] if g.client in self.routes else []
            self.send_queue.put_nowait((deadline, g.client, routes, s))
            self.is_send_queue_changed = True
            log.debug("New immediate set deadline: %s", to_human(deadline))

    def on_set(self, log: Logger, s: SetItem):
        log = ContextLogger(log, f"set {s.label}@{s.at}")
        if s.label not in self.interests:
            log.warning("Received set for an unknown interest")

        # Check for previous content entry
        try:
            if self.content_store[s.label].at >= s.at:
                log.debug("Ignored old publications")
                return
            last = self.content_store[s.label].last
            fulfil = self.content_store[s.label].fulfil
        except KeyError:
            log.debug("New label in content store")
            last = 0
            fulfil = None

        # Insert new entry
        self.content_store[s.label] = s
        self.content_store[s.label].last = last
        log.info("Updated local content store")

        # Fulfil any local interests (applications waiting in .get())
        if fulfil is not None:
            fulfil.set_result(True)

        # Add sets towards interested clients to queue
        for ttp, client in s.dst:
            if self.advert is None or self.advert.client != client:
                deadline = time.time() + ttp
                new_set_item = SetItem(s.label, s.data, s.at, [(ttp, client)])
                routes = self.routes[client] \
                    if client in self.routes else []
                self.send_queue.put_nowait(
                    (deadline, client, routes, new_set_item))
                self.is_send_queue_changed = True
