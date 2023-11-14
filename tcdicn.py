import asyncio
import json
import logging
import queue
import signal
import socket
import time
from asyncio import DatagramTransport, StreamWriter, StreamReader, Future
from logging import Logger, LoggerAdapter
from json import JSONDecodeError
from typing import Dict, Tuple, List

# The version of this protocol implementation is included in all communications
# This allows peers which implement one or more versions to react appropriately
VERSION: str = "0.2-dev"

# The soft maximum size in bytes to allow batched client advert forwarding
# broadcasts, which limits the number of client adverts sent at once
# Generally a good idea set to less than the Maximum Transmission Unit (MTU)
# This minimises the chance of peer advert broadcasts being dropped/lost
ADVERT_CAPACITY: int = 512

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
def do_after(eol: float, callback) -> asyncio.Task:
    async def on_timeout():
        await asyncio.sleep(eol - time.time())
        callback()
    return asyncio.create_task(on_timeout())


# Nodes commutate using messages which can be sent over TCP or UDP
# A Message is only a JSON formatted version number and list of MessageItems
# JSON field names are shrunk to help pack more information into UDP datagrams

# This class is just define a common type between MessageItems
class MessageItem:
    def to_dict(): raise NotImplementedError
    def from_dict(): raise NotImplementedError


# Lets other nodes know what time your End Of Life (EOL) is
# Without further PeerItems, you will be forgotten from the network after EOL
# As such, these should be broadcasted over UDP at regular intervals before EOL
class PeerItem(MessageItem):
    def __init__(self, eol: float):
        self.eol = eol

    def to_dict(self):
        return {
            "t": "p",
            "e": self.eol,
        }

    def from_dict(d):
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

    def to_dict(self):
        return {
            "t": "a",
            "c": self.client,
            "l": self.labels,
            "s": self.score,
            "p": self.ttp,
            "e": self.eol,
        }

    def from_dict(d):
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

    def to_dict(self):
        return {
            "t": "g",
            "c": self.client,
            "l": self.label,
            "a": self.after,
            "p": self.ttp,
            "e": self.eol,
        }

    def from_dict(d):
        if d["t"] != "g":
            raise ValueError("Not a get request message item")
        return GetItem(d["c"], d["l"], d["a"], d["p"], d["e"])


# Request to cache and propagate the contained data towards interested clients
# Time To Propagate (TTP) demands that nodes wait no more than TTP seconds
# before propagating this SetItem towards subscribers (due to batching reasons)
class SetItem:
    def __init__(self, label: str, data: str, at: float, ttp: float):
        self.label = label
        self.data = data
        self.at = at
        self.ttp = ttp

    def to_dict(self):
        return {
            "t": "s",
            "l": self.label,
            "d": self.data,
            "a": self.at,
            "p": self.ttp,
        }

    def from_dict(d):
        if d["t"] != "s":
            raise ValueError("Not a set request message item")
        return SetItem(d["l"], d["d"], d["a"], d["p"])


# The data structure passed between nodes on the network in JSON format
class Message:
    def __init__(self, items: List[MessageItem]):
        self.version = VERSION
        self.items = items

    def to_dict(self):
        return {
            "v": VERSION,
            "i": [item.to_dict() for item in self.items]
        }

    def from_dict(d):
        if d["v"] != VERSION:
            raise ValueError("Message version unsupported:", d["v"])
        t_map = {"p": PeerItem, "a": AdvertItem, "g": GetItem, "s": SetItem}
        return Message([t_map[item["t"]].from_dict(item) for item in d["i"]])

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode()

    def from_bytes(data: bytes):
        return Message.from_dict(json.loads(data))


class ClientInfo:
    def __init__(
            self, name: str, labels: List[str],
            ttp: float, score: float = 1000):
        self.name = name
        self.labels = labels
        self.ttp = ttp
        self.score = score

    def to_advert(self, eol: float) -> AdvertItem:
        return AdvertItem(self.name, self.labels, self.score, self.ttp, eol)


class Peer:
    def __init__(self):
        self.timer = None


class Client:
    def __init__(self):
        self.eol = 0
        self.timer = None


class Interest:
    pass


# Entries in the content store are keyed by their label
# In addition to the data, they contain when the data was published and when
# the data was viewed by the client, so that not-seen-before data can be gotten
# The fulfil future allows calls to node.get() to wait for new data to arrive
class Content:
    def __init__(self):
        self.data: str = None
        self.at: float = 0
        self.last: float = 0
        self.fulfil: Future = None


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
        self.log = logging.getLogger(__name__)
        self.peers: Dict[Addr, Peer] = {}
        self.clients: Dict[str, Client] = {}
        self.interests: Dict[str, Interest] = {}

        # TODO(optimisation): write to/read from disk
        self.content_store: Dict[str, Content] = {}

        self.batch_fwd_adverts_task = None
        self.adverts_queue = queue.PriorityQueue()
        self.is_adverts_queue_changed = False

    # Starts all tasks needed for the node to communicate with the network
    # Send the process a SIGINT or cancel the coroutine to shutdown the node
    async def start(
            self, port: int, dport: int,
            ttl: float, tpf: int,
            client: ClientInfo = None):
        self.port = port
        self.dport = dport
        self.ttl = ttl
        self.client = client

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
            UdpProtocol,
            local_addr=("0.0.0.0", self.port),
            allow_broadcast=True)
        self.tcp = await asyncio.start_server(
            self.on_connection, "0.0.0.0", self.port)

        # Regularly broadcast own adverts TPF times before our TTL can run out
        async def do_regular_broadcasts():
            while True:
                try:
                    self.log.debug("Broadcasting advert...")
                    eol = time.time() + ttl
                    adverts = [] if client is None else [client.to_advert(eol)]
                    self.broadcast_msg(Message([PeerItem(eol)] + adverts))
                except OSError as e:
                    self.log.warning("Error broadcasting: %s", e)
                await asyncio.sleep(ttl / tpf)

        # Run in background
        tcp_task = asyncio.create_task(self.tcp.serve_forever())
        reg_task = asyncio.create_task(do_regular_broadcasts())
        tasks = [tcp_task, reg_task]

        # Shutdown if we receive a signal
        def shutdown():
            logging.info("Shutting down...")
            reg_task.cancel()
            self.udp.close()
            self.tcp.close()
        for sig in [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]:
            loop.add_signal_handler(sig, shutdown)

        # Wait until cancelled or shutdown
        try:
            async with self.tcp:
                logging.info("Up and listening on :%s", self.port)
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.exceptions.CancelledError:
            logging.debug("Node tasks cancelled")
        logging.info("Goodbye :)")

    # Subscribes to label and returns first new value received
    # Repeats request every TTL/TPF seconds until successful or cancelled
    # Allows each intermediate node to batch responses for up to TTP seconds
    async def get(self, label: str, ttl: float, tpf: int, ttp: float) -> str:
        log = ContextLogger(self.log, f"get {label}")
        if self.client is None:
            raise RuntimeError("Only client nodes can subscribe")

        # Check if local content store already has a new value
        if label not in self.content_store:
            self.content_store[label] = Content()
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
                client = self.client.client
                after = self.content_store[label].last
                while True:
                    log.debug("Sending get request...")
                    eol = time.time() + ttl
                    self.on_get(log, GetItem(client, label, after, ttp, eol))
                    asyncio.sleep(ttl / ttp)
            task = asyncio.create_task(subscribe())
            assert await self.content_store[label].fulfil
            task.cancel()

        self.content_store[label].last = self.content_store[label].at
        return self.content_store[label].data

    # Publishes a new value to a label
    # This will only be propagated towards interested clients
    async def set(self, label: str, data: str):
        log = ContextLogger(self.log, f"set {label}")
        log.info("Publishing to %s...", label)
        self.on_set(log, SetItem(label, data, time.time()))

    # Batching

    def schedule_batch_fwd_adverts(self):

        # Check for previous scheduled batch
        if self.batch_fwd_adverts_task is not None:
            self.batch_fwd_adverts_task.cancel()
            self.batch_fwd_adverts_task = None

        # Find next advert forwarding deadline
        try:
            deadline, advert = self.adverts_queue.get_nowait()
            self.adverts_queue.put_nowait((deadline, advert))
        except queue.Empty:
            return

        # Schedule new time
        now = time.time()
        eol = (deadline - now) / 2 + now
        task = do_after(eol, self.batch_fwd_adverts)
        self.batch_fwd_adverts_task = task
        self.log.debug(
            "Scheduled next adverts fwd batch: %s (for %s)",
            to_human(eol), advert.client)

    def batch_fwd_adverts(self):
        log = ContextLogger(self.log, "adverts fwd")

        items = []
        msg = Message([])
        msg_bytes = msg.to_bytes()
        msg_len = len(msg_bytes)
        clients = set()

        # Add as many pending adverts to this broadcast as we safely can
        # Force at least one if this is any to prevent queue blocking
        while True:
            try:
                deadline, advert = self.adverts_queue.get_nowait()
            except queue.Empty:
                break
            if advert.client in clients:
                log.warning("Dropping duplicate advert for %s", advert.client)
                continue

            new_items = items + [advert]
            new_msg = Message(new_items)
            new_msg_bytes = new_msg.to_bytes()
            new_msg_len = len(new_msg_bytes)
            diff = new_msg_len - msg_len
            if len(items) != 0 and new_msg_len >= ADVERT_CAPACITY:
                log.debug("Refused %s advert (+%s bytes)", advert.client, diff)
                self.adverts_queue.put_nowait((deadline, advert))
                break
            log.debug("Added %s advert (+%s bytes)", advert.client, diff)

            items = new_items
            msg = new_msg
            msg_bytes = new_msg_bytes
            msg_len = new_msg_len
            clients.add(advert.client)

        # Send it!
        try:
            self.broadcast_msg(msg)
        except OSError as e:
            log.warning("Error broadcasting adverts batch: %s", e)
            # TODO(safety): this is extremely likely to explode
            # it would be nice to have a limit on adverts in general
            # for advert in msg.items:
            #    self.adverts_queue.put_nowait((0, advert))

        # Schedule next batch
        self.schedule_batch_fwd_adverts()

    # Network methods - May raise OSError

    async def send_msg(self, addr: Addr, msg: Message):
        msg_bytes = msg.to_bytes()
        _, writer = await asyncio.open_connection(addr[0], addr[1])
        writer.write(msg_bytes)
        await writer.drain()
        writer.close()
        self.log.debug(
            "Sent %s: %s (%s bytes)",
            addr, len(msg.items), len(msg_bytes))

    def broadcast_msg(self, msg: Message):
        msg_bytes = msg.to_bytes()
        self.udp.sendto(msg_bytes, ("<broadcast>", self.dport))
        self.log.debug(
            "Broadcast: %s (%s bytes)",
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
        # TODO(safety): limit amount and time
        try:
            data = await reader.read()
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
            log.warning("Ignored malformed message %s")
            return
        if msg.version != VERSION:
            log.warning("Ignored message with version %s", msg.version)
            return

        # Handle message items appropriately
        for item in msg.items:
            if type(item) is PeerItem:
                self.on_peer(log, addr, item)
            if type(item) is AdvertItem:
                self.on_advert(log, addr, item)
            elif type(item) is GetItem:
                self.on_get(log, item)
            elif type(item) is SetItem:
                self.on_set(log, item)

        # Reschedule next adverts forwarding batch if necessary
        if self.is_adverts_queue_changed:
            self.schedule_batch_fwd_adverts()
            self.is_adverts_queue_changed = False

    # Handlers for each MessageItem type
    # If called directly, it is your responsibility to refresh any
    # scheduled tasks that should be affected (see on_message)

    def on_peer(self, log: Logger, addr: Addr, peer: PeerItem):
        log = ContextLogger(log, "peer")

        # Check for previous peer entry
        try:
            self.peers[addr].timer.cancel()
        except KeyError:
            log.info("New peer")
            self.peers[addr] = Peer()

        # Insert new peer entry with timeout
        def on_timeout():
            log.info("Timed out peer")
            del self.peers[addr]
        timer = do_after(peer.eol, on_timeout)
        self.peers[addr].eol = peer.eol
        self.peers[addr].timer = timer

    def on_advert(self, log: Logger, addr: Addr, advert: AdvertItem):
        log = ContextLogger(log, f"{advert.client}")

        # Check for previous entry
        try:
            if advert.eol <= self.clients[advert.client].eol:
                log.debug("Ignored old advert")
                return
            self.clients[advert.client].timer.cancel()
        except KeyError:
            log.info("New client")
            self.clients[advert.client] = Client()
            # TODO(optimisation): use batching+retrying
            # TODO(v0.2): forward interests to new clients

        # Insert new entry with timeout
        def on_timeout():
            log.info("Timed out client")
            del self.clients[advert.client]
        timer = do_after(advert.eol, on_timeout)
        self.clients[advert.client].eol = advert.eol
        self.clients[advert.client].timer = timer

        # Add advert
        deadline = time.time() + advert.ttp
        self.adverts_queue.put_nowait((deadline, advert))
        self.is_adverts_queue_changed = True
        log.debug("New advert deadline: %s", to_human(deadline))

        # TODO(safely): cooldown
        # TODO(safely): issue warning if duplicate name spotted

    def on_get(self, log: Logger, g: GetItem):
        log = ContextLogger(log, f"get {g.label}>{g.after}@{g.client}")

        # TODO(v0.2): interests

        # Check for previous entry
        if g.label not in self.interests:
            log.info("New interest label")
            self.interests[g.label] = {}
        try:
            if g.eol <= self.interests[g.label][g.client].eol:
                log.debug("Ignored old interest")
                return
            self.interests[g.label][g.client].timer.cancel()
        except KeyError:
            log.info("New interest label client")
            self.interests[g.label][g.client] = {}

        # Insert new entry with timeout
        def on_timeout():
            log.info("Timed out")
            del self.interests[g.label][g.client]
            if len(self.interests[g.label]) == 0:
                log.info("No more interest in %s", g.label)
                del self.interests[g.label]
        timer = do_after(g.eol, on_timeout)
        self.interests[g.label][g.client].eol = g.eol
        self.interests[g.label][g.client].timer = timer

        # TODO(v0.2) forward interests towards relevant clients with batching

    def on_set(self, log: Logger, s: SetItem):
        log = ContextLogger(log, f"set {s.label}@{s.at}")

        try:
            if self.content_store[s.label].at >= s.at:
                log.debug("Ignored old publications")
                return
        except KeyError:
            log.debug("New label in content store")
            self.content_store[s.label] = Content()

        self.content_store[s.label].data = s.data
        self.content_store[s.label].at = s.at
        log.debug("Updated local content store")

        # TODO(optimisation) use batching+retrying with ttp set to interest ttp
        # TODO(v0.2) send towards only interested clients
        msg = Message([s])
        for peer in self.peers:
            asyncio.create_task(self.send_msg(peer, msg))

        # Fulfil any local interests (applications waiting in .get())
        if self.content_store[s.label].fulfil is not None:
            self.content_store[s.label].fulfil.set_result(True)
