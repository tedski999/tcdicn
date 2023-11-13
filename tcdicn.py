import asyncio
import json
import logging
import queue
import signal
import socket
import time
from asyncio import DatagramTransport, StreamWriter, StreamReader
from logging import Logger, LoggerAdapter
from json import JSONDecodeError
from typing import Tuple

# The version of this protocol implementation is included in all communications
# This allows peers which implement one or more versions to react appropriately
VERSION: str = "0.2-dev"

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


# Construct a transmissible message
def encode_msg(msg: dict) -> bytes:
    return json.dumps(msg, separators=(",", ":")).encode()


# Construct a transmissible peer advert message
def serialise_advert_msg(eol: float, client_adverts: dict) -> bytes:
    return encode_msg({
        "v": VERSION,
        "t": "advert",
        "e": eol,
        "a": {
            client: {
                "p": client_advert["ttp"],
                "e": client_advert["eol"],
                "t": client_advert["tags"],
                "s": client_advert["score"],
            } for client, client_advert in client_adverts.items()
        }
    })


# Construct dict from transmission message
def decode_msg(msg: bytes) -> dict:
    return json.loads(msg)


# Convert peer advert and inner client adverts into a more useable format
def parse_peer_advert(msg: dict) -> dict:
    advert = {
        "version": str(msg["v"]),
        "type": str(msg["t"]),
        "eol": float(msg["e"]),
        "client_adverts": {},
    }
    for client, client_msg in msg["a"].items():
        client_advert = {
            "ttp": float(client_msg["p"]),
            "eol": float(client_msg["e"]),
            "tags": list(client_msg["t"]),
            "score": float(client_msg["s"])
        }
        advert["client_adverts"][client] = client_advert
    return advert


# Convert TCP message into a more useable format
def parse_msg(msg: dict) -> dict:
    if msg["t"] == "get":
        return {
            "version": str(msg["v"]),
            "type": str(msg["t"]),
            "ttp": str(msg["p"]),
            "eol": str(msg["e"]),
            "tag": str(msg["n"]),
            "after": str(msg["a"]),
            "client": str(msg["c"])
        }
    elif msg["t"] == "set":
        return {
            "version": str(msg["v"]),
            "type": str(msg["t"]),
            "tag": str(msg["n"]),
            "data": str(msg["d"]),
            "at": str(msg["a"])
        }
    else:
        return {
            "version": str(msg["v"]),
            "type": str(msg["t"]),
        }


# TODO(comment)
async def send_set(addr: Addr, tag: str, data: str, at: float):
    _, writer = await asyncio.open_connection(*addr)
    writer.write(json.dumps({
        "v": VERSION,
        "t": "set",
        "n": tag,
        "d": data,
        "a": at
    }).encode())
    await writer.drain()
    writer.close()


# TODO(comment)
async def send_get(
        addr: Addr, tag: str, after: float,
        ttp: float, eol: float, client: str):
    _, writer = await asyncio.open_connection(*addr)
    writer.write(json.dumps({
        "v": VERSION,
        "t": "set",
        "n": tag,
        "a": after,
        "p": ttp,
        "e": eol,
        "c": client
    }).encode())
    await writer.drain()
    writer.close()


# TODO(comment)
class Protocol:

    def __init__(
            self, sport: int, dport: int,
            ttl: float, tpf: int, client=None):
        self.log = logging.getLogger(__name__)
        self.sport = sport  # Port to send from
        self.dport = dport  # Port to send to
        self.ttl = ttl  # Time To Live for peer adverts
        self.tpf = tpf  # How many peer adverts to send before TTL (PreFire)

        # TODO(comment)
        self.content = {}

        # TODO(routing): never forward to where a message came from
        # or where it has already been sent

        # Tables for both peers and clients this node currently knows about
        # Includes information such as its End Of Life (EOL) time as well as
        # the timeout task which will eventually remove it from the table if
        # not canceled before the entries EOL
        self.known_peers = {}
        self.known_clients = {}
        self.known_interests = {}

        # Broadcasts have a max bytes capacity of ADVERT_CAPACITY and a max
        # interval of TTL/TPF before we need to broadcast a peer advert
        # To respect any received client adverts Time To Propagate (TTP),
        # they are inserted into a priority queue based on their TTP, which
        # lets us use schedule broadcasts such that one is sent in time for the
        # next TTP deadline / 2 and then includes as many client adverts as it
        # can fit, with each client advert taken in order of their TTP
        self.next_broadcast_task = None
        self.client_adverts_queue = queue.PriorityQueue()

        # Client advert for this node - If not a client, left empty
        # This is regularly inserted into the client adverts queue to ensure
        # a client advert is made on average at least every TTL/TPF seconds
        self.advert = None if client is None else {
            "client": client["name"],
            "advert": {
                "ttp": client["ttp"],
                "tags": client["tags"],
                "score": 1000,
            }
        }

    # Send peer broadcasts regularly once the UDP transport is established
    def connection_made(self, udp: DatagramTransport):
        self.log.debug("UDP transport established on :%s", self.sport)
        self.udp = udp

        async def do_fixed_broadcasts():
            while True:
                if self.advert is not None:
                    self.advert["advert"]["eol"] = time.time() + self.ttl
                    self.client_adverts_queue.put_nowait((0, self.advert))
                self.broadcast_advert()
                await asyncio.sleep(self.ttl / self.tpf)
        self.fixed_broadcasts_task = asyncio.create_task(do_fixed_broadcasts())

    # Shutdown the protocol
    def connection_lost(self, exc: Exception):
        if exc is None:
            self.log.debug("UDP transport closed")
        else:
            self.log.warning("UDP transport lost: %s", exc)
        if self.next_broadcast_task is not None:
            self.next_broadcast_task.cancel()
        if self.fixed_broadcasts_task is not None:
            self.fixed_broadcasts_task.cancel()
        for peer in self.known_peers.values():
            peer["timer"].cancel()
        for client in self.known_clients.values():
            client["timer"].cancel()

    # Log transport errors
    def error_received(self, exc: OSError):
        self.log.warning("UDP transport error: %s", exc)

    # Process any incoming UDP datagrams as peer adverts
    def datagram_received(self, msg: bytes, peer: Addr):
        log = ContextLogger(self.log, f"UDP {peer[0]}:{peer[1]}")

        # Ignore our own broadcasts
        l_addrs = socket.getaddrinfo(socket.gethostname(), self.sport)
        r_addrs = socket.getaddrinfo(socket.getfqdn(peer[0]), peer[1])
        for (_, _, _, _, l_addr) in l_addrs:
            for (_, _, _, _, r_addr) in r_addrs:
                if r_addr == l_addr:
                    log.debug("Ignored broadcast from self")
                    return

        # Parse peer advert message
        try:
            advert = parse_peer_advert(json.loads(msg))
        except (JSONDecodeError, KeyError, ValueError):
            log.warning("Ignored malformed message")
            return
        if advert["version"] != VERSION:
            log.warning("Ignored message version: %s", advert["version"])
            return
        if advert["type"] != "advert":
            log.warning("Ignored message type: %s", msg["type"])
            return

        # Update known peers and clients with new info and timeouts
        self.use_peer_advert(log, advert, peer)

    # Update known peers and clients with new info and timeouts
    def use_peer_advert(self, log: Logger, advert: dict, peer: Addr):

        # Cancel previous timeout
        try:
            self.known_peers[peer]["timer"].cancel()
        except KeyError:
            log.info("New peer")
            self.known_peers[peer] = {}

        # Insert new peer info and timeout into known peers
        def timeout():
            log.info("Peer timed out")
            del self.known_peers[peer]
        timer = do_after(advert["eol"], timeout)
        self.known_peers[peer]["timer"] = timer
        self.known_peers[peer]["eol"] = advert["eol"]
        log.debug("New timeout: %s", to_human(advert["eol"]))

        # Update known clients with new clients info and timeouts
        for client, client_advert in advert["client_adverts"].items():
            self.use_client_advert(log, peer, client, client_advert)

        # Maybe update time to next broadcast to accommodate new clients' TTP
        self.schedule_next_broadcast()

    # Update known clients with new client info and timeout
    def use_client_advert(
            self, log: Logger, peer: Addr, client: str, client_advert: dict):
        log = ContextLogger(log, f"{client}")

        # Cancel previous timeout
        try:
            if self.known_clients[client]["eol"] >= client_advert["eol"]:
                log.debug("Ignored already seen client advert")
                return
            self.known_clients[client]["timer"].cancel()
        except KeyError:
            log.info("New client")
            self.known_clients[client] = {}

        # Insert new client info and timeout into known clients
        def timeout():
            log.info("Client timed out")
            del self.known_clients[client]
            for peer in self.known_peers:
                if client in self.known_peers[peer]["routes"]:
                    del self.known_peers[peer]["routes"][client]
        timer = do_after(client_advert["eol"], timeout)
        self.known_clients[client]["timer"] = timer
        self.known_clients[client]["eol"] = client_advert["eol"]
        self.known_clients[client]["tags"] = client_advert["tags"]
        log.debug("New timeout: %s", to_human(client_advert["eol"]))

        # Insert new route score for this client via this peer
        if "routes" not in self.known_peers[peer]:
            self.known_peers[peer]["routes"] = {}
        self.known_peers[peer]["routes"][client] = client_advert["score"]
        log.debug("New route score: %s", client_advert["score"])

        # Push any relevant interests towards client with batching
        for tag in self.known_clients[client]["tags"]:
            pass  # TODO(batching): if interest exists, add to queue

        # Push any relevant fulfilments towards client with batching
        if client in self.known_interests:
            for tag in self.known_interests[client]:
                pass  # TODO(batching): if can be fulfilled, add to queue

        # Insert new client advert into the client adverts priority queue
        deadline = time.time() + client_advert["ttp"]
        if client_advert["eol"] < deadline:
            log.debug("Not propagating advert as EOL < TTP")
            return
        client_item = {"client": client, "advert": client_advert}
        self.client_adverts_queue.put_nowait((deadline, client_item))
        log.debug("Advert deadline: %s", to_human(deadline))

    # Broadcast a peer advert containing as many pending client adverts as can
    # fit within ADVERT_CAPACITY bytes, taken in order of their TTP deadlines
    def broadcast_advert(self):
        log = ContextLogger(self.log, "UDP Broadcast")

        eol = time.time() + self.ttl
        client_adverts = {}
        msg_len = len(serialise_advert_msg(eol, client_adverts))
        log.debug("Created peer advert (%s bytes)", msg_len)

        # Add as many pending client adverts to this broadcast as we safely can
        while True:

            # Get the next pending client advert in the queue if any
            try:
                deadline, client_item = self.client_adverts_queue.get_nowait()
            except queue.Empty:
                break
            client = client_item["client"]
            client_advert = client_item["advert"]

            # Attempt to fit client advert into peer advert
            # Force it if this is the first (and as such only) client advert
            new_client_adverts = {**client_adverts, client: client_advert}
            new_msg_len = len(serialise_advert_msg(eol, new_client_adverts))
            len_diff = new_msg_len - msg_len
            if len(client_adverts) != 0 and new_msg_len >= ADVERT_CAPACITY:
                log.debug("Not adding %s client advert (+%s bytes)", len_diff)
                self.client_adverts_queue.put_nowait((deadline, client_item))
                break
            log.debug("Added %s client advert (+%s bytes)", client, len_diff)
            client_adverts = new_client_adverts
            msg_len = new_msg_len

        # Send it!
        msg = serialise_advert_msg(eol, client_adverts)
        try:
            self.udp.sendto(msg, ("<broadcast>", self.dport))
        except OSError as exc:
            log.error("%s", exc)
        log.debug("Sent %s bytes total with EOL %s", len(msg), to_human(eol))

        # Schedule next broadcast for self or remaining pending client adverts
        self.schedule_next_broadcast()

    # Determine when to schedule the next peer advert broadcast based on the
    # time until half the time for the next client advert TTP deadline
    def schedule_next_broadcast(self):

        # Replace previous broadcast task
        if self.next_broadcast_task is not None:
            self.next_broadcast_task.cancel()
            self.next_broadcast_task = None

        # Find the time of the next advert deadline
        try:
            deadline, client_item = self.client_adverts_queue.get_nowait()
            self.client_adverts_queue.put_nowait((deadline, client_item))
        except queue.Empty:
            return

        # Scheduled new broadcast time
        now = time.time()
        deadline = (deadline - now) / 2 + now
        self.next_broadcast_task = do_after(deadline, self.broadcast_advert)
        self.log.debug(
            "Next broadcast time set: %s (%s advert)",
            to_human(deadline), client_item["client"])

    # Handle TCP connections
    async def on_connection(self, reader: StreamReader, writer: StreamWriter):
        peer = writer.get_extra_info("peername")[0:2]
        log = ContextLogger(self.log, f"TCP {peer[0]}:{peer[1]}")
        log.debug("New connection")

        # Read entire message
        try:
            msg = await reader.read()
        except Exception as exc:
            log.warning("Error reading message: %s", exc)
            return
        finally:
            writer.close()

        # Parse message
        try:
            msg = parse_msg(decode_msg(msg))
        except (JSONDecodeError, KeyError, ValueError):
            log.warning("Ignored malformed message")
            return
        if msg["version"] != VERSION:
            log.warning("Ignored message version: %s", msg["version"])
            return

        # Use message
        self.use_msg(log, msg)

    # TODO(comment)
    async def use_msg(self, log: Logger, msg: dict):

        # Use message
        if msg["type"] == "get":
            self.use_get_msg(log, msg)
        elif msg["type"] == "set":
            self.use_set_msg(log, msg)
        else:
            log.warning("Ignored message type: %s", msg["type"])
            return

        # TODO(routing): check if can now fulfil interest with batching
        # Fulfilling should update the known_interests to the max()

        # TODO(temp): temporary gossip to test network
        for tag in self.known_interests:
            if tag not in self.content:
                continue
            if self.content[tag]["at"] <= self.known_interests[tag]["after"]:
                continue
            log = ContextLogger(log, f"gossip {tag}")
            for peer in self.known_peers:
                log.info("to %s", peer)
                data = self.content[tag]["data"]
                at = self.content[tag]["at"]
                try:
                    await send_set(peer, tag, data, at)
                except OSError as exc:
                    log.error("%s", exc)
            self.known_interests[tag]["after"] = self.content[tag]["at"]

    # TODO(comment)
    def use_get_msg(self, log: Logger, msg: dict):
        tag, client = msg["tag"], msg["client"]
        after, eol = msg["after"], msg["eol"]
        log = ContextLogger(log, f"get {tag}@{client}")

        # Cancel previous timeout
        try:
            if self.known_interests[client]["eol"] >= eol:
                log.debug("Ignored already known interest")
                return
            self.known_interests[client]["timer"].cancel()
        except KeyError:
            log.info("New interest")
            self.known_interests[client] = {}

        # Insert new interest and timeout into known interests
        def timeout():
            log.info("Interest timed out")
            del self.known_interests[client]
        timer = do_after(eol, timeout)
        self.known_interests[client]["timer"] = timer
        self.known_interests[client]["eol"] = eol
        self.known_interests[client]["tag"] = tag
        self.known_interests[client]["after"] = after
        log.debug("New timeout: %s", to_human(eol))
        log.debug("New filter: %s", to_human(after))

        # TODO(routing): push interest towards known client publishers
        # via best path with retry and batching

    # TODO(comment)
    def use_set_msg(self, log: Logger, msg: dict):
        tag, data, at = msg["tag"], msg["data"], msg["at"]
        log = ContextLogger(log, f"set {tag}")

        # Ignore old data due to network loops
        try:
            if at <= self.content[tag]["at"]:
                log.debug("Ignored already seen data")
                return
        except KeyError:
            log.debug("New tag")
            self.content[tag] = {}

        # Insert new data into content store
        self.content[tag]["data"] = data
        self.content[tag]["at"] = at
        log.debug("New data published %s", at)

        # Fulfill any clients waiting on this data
        # TODO(client)


# TODO(comment)
class Node:
    def __init__(self):
        self.proto = asyncio.get_running_loop().create_future()

    # TODO(comment)
    async def start(
            self, sport: int, dport: int,
            ttl: float, tpf: int, client=None):
        loop = asyncio.get_running_loop()

        # Start UDP and TCP server running the ICN protocol
        udp, proto = await loop.create_datagram_endpoint(
            lambda: Protocol(sport, dport, ttl, tpf, client),
            local_addr=("0.0.0.0", sport), allow_broadcast=True)
        tcp = await asyncio.start_server(
            proto.on_connection, "0.0.0.0", sport)
        self.proto.set_result(proto)

        # Shutdown if we receive a signal
        def shutdown():
            logging.info("Shutting down...")
            udp.close()
            tcp.close()
        for sig in [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]:
            loop.add_signal_handler(sig, shutdown)

        # Run until cancelled or shutdown
        try:
            async with tcp:
                logging.info("Up and listening on :%s", sport)
                await tcp.serve_forever()
        except asyncio.exceptions.CancelledError:
            logging.debug("Node tasks cancelled")
        logging.info("Goodbye :)")

    # TODO(comment)
    async def get(self, tag: str, ttl: float, tpf: int, ttp: float) -> str:
        proto = await self.proto
        if proto.advert is None:
            raise RuntimeError("Only ICN clients can subscribe to the network")

        msg = {
            "version": VERSION,
            "type": "get",
            "tag": tag,
            "ttp": ttp,
            "after": proto.content[tag]["at"] if tag in proto.content else 0,
            "client": proto.advert["client"]
        }

        # TODO(client) await future tied to content

        while True:
            msg["eol"] = time.time() + ttl
            await proto.use_msg(logging.getLogger(__name__), msg)
            await asyncio.sleep(ttl / tpf)

        return "TODO"

    # TODO(comment)
    async def set(self, tag: str, data: str):
        proto = await self.proto
        msg = {
            "version": VERSION,
            "type": "set",
            "tag": tag,
            "data": data,
            "at": time.time()
        }
        await proto.use_msg(logging.getLogger(__name__), msg)
