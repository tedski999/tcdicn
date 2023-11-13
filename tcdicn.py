import asyncio
import json
import logging
import queue
import signal
import socket
import time
from asyncio import DatagramTransport, DatagramProtocol
from asyncio import StreamWriter, StreamReader
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
class ContextLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"{self.extra} | {msg}", kwargs


# Convert a UNIX timestamp into a human readable seconds since string
def to_human(timestamp: float):
    secs = timestamp - time.time()
    return f"in {secs} seconds" if secs >= 0 else f"{-secs} seconds ago"


# Execute callback after End Of Life timestamp - Useful for implementing caches
def do_after(eol: float, callback):
    async def on_timeout():
        await asyncio.sleep(eol - time.time())
        callback()
    return asyncio.create_task(on_timeout())


# Construct a transmissible message
def encode_msg(msg: dict) -> bytes:
    return json.dumps(msg, separators=(",", ":")).encode()


# Construct a transmissible peer advert message
def encode_advert_msg(eol: float, client_adverts: dict):
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


# Convert peer advert and inner client adverts into a more useable format
def parse_peer_advert(msg: dict) -> dict:
    advert = {
        "version": str(msg["v"]),
        "type": str(msg["t"]),
        "eol": float(msg["e"]),
        "client_adverts": {},
    }
    for client, client_msg in msg["a"].items():
        client_advert = parse_client_advert(client_msg)
        advert["client_adverts"][client] = client_advert
    return advert


# Convert client advert into a more useable format
def parse_client_advert(msg: dict) -> dict:
    client_advert = {}
    client_advert["ttp"] = float(msg["p"])
    client_advert["eol"] = float(msg["e"])
    client_advert["tags"] = list(msg["t"])
    client_advert["score"] = float(msg["s"])
    return client_advert


class UdpProtocol(DatagramProtocol):

    def __init__(
            self, sport: int, dport: int,
            ttl: float, tpf: int, client=None):
        self.log = ContextLogger(logging.getLogger(__name__), f"UDP:{sport}")
        self.sport = sport  # Port to send from and listen on
        self.dport = dport  # Port to send to
        self.ttl = ttl  # Time To Live for peer adverts
        self.tpf = tpf  # How many peer adverts to send before TTL (PreFire)

        # Tables for both peers and clients this node currently knows about
        # Includes information such as its End Of Life (EOL) time as well as
        # the timeout task which will eventually remove it from the table if
        # not canceled before the entries EOL
        self.known_peers = {}
        self.known_clients = {}

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
        # a peer advert is made on average at least every TTL/TPF seconds
        self.advert = {
            "client": "self" if client is None else client["name"],
            "advert": None if client is None else {
                "ttp": client["ttp"],
                "tags": client["tags"],
                "score": 1000,
            }
        }

    # Send our first broadcast once the UDP transport is established
    def connection_made(self, udp: DatagramTransport):
        self.log.debug("Transport established")
        self.udp = udp
        self.client_adverts_queue.put_nowait((time.time(), self.advert))
        self.broadcast_advert()

    # Shutdown the protocol
    def connection_lost(self, exc: Exception):
        self.log.error("Transport lost: %s", exc)
        self.udp.close()
        if self.next_broadcast_task is not None:
            self.next_broadcast_task.cancel()
        for peer in self.known_peers.values():
            peer["timer"].cancel()
        for client in self.known_clients.values():
            client["timer"].cancel()

    # Log transport errors
    def error_received(self, exc: OSError):
        self.log.warning("Transport error: %s", exc)

    # Process any incoming UDP datagrams as peer adverts
    def datagram_received(self, msg: bytes, peer: Addr):
        peer_log = ContextLogger(self.log, f"{peer[0]}:{peer[1]}")

        # Ignore our own broadcasts
        l_addrs = socket.getaddrinfo(socket.gethostname(), self.sport)
        r_addrs = socket.getaddrinfo(socket.getfqdn(peer[0]), peer[1])
        for (_, _, _, _, l_addr) in l_addrs:
            for (_, _, _, _, r_addr) in r_addrs:
                if r_addr == l_addr:
                    peer_log.debug("Ignored broadcast from self")
                    return

        # Parse peer advert message
        try:
            advert = json.loads(msg)
            advert = parse_peer_advert(advert)
        except JSONDecodeError or KeyError:
            peer_log.warning("Ignored malformed message")
            return
        if advert["version"] != VERSION:
            peer_log.warning("Ignored message version: %s", advert["version"])
            return
        if advert["type"] != "advert":
            peer_log.warning("Ignored message type: %s", type)
            return

        # Update known peers and clients with new info and timeouts
        self.use_peer_advert(advert, peer)

    # Update known peers and clients with new info and timeouts
    def use_peer_advert(self, advert: dict, peer: Addr):
        peer_log = ContextLogger(self.log, f"{peer[0]}:{peer[1]}")

        # Cancel previous timeout
        try:
            self.known_peers[peer]["timer"].cancel()
        except KeyError:
            peer_log.info("New peer")
            self.known_peers[peer] = {}

        # Insert new peer info and timeout into known peers
        def timeout_peer():
            peer_log.info("Peer timed out")
            del self.known_peers[peer]
            # TODO: redo fib
        timer = do_after(advert["eol"], timeout_peer)
        self.known_peers[peer]["timer"] = timer
        self.known_peers[peer]["eol"] = advert["eol"]
        peer_log.debug("New timeout: %s", to_human(advert["eol"]))

        # Update known clients with new clients info and timeouts
        for client, client_advert in advert["client_adverts"].items():
            self.use_client_advert(peer, client, client_advert)

        # Maybe update time to next broadcast to accommodate new clients' TTP
        self.schedule_next_broadcast()

    # Update known clients with new client info and timeout
    def use_client_advert(self, peer: Addr, client: str, client_advert: dict):
        peer_log = ContextLogger(self.log, f"{peer[0]}:{peer[1]}")
        client_log = ContextLogger(peer_log, f"{client}")

        # Cancel previous timeout
        try:
            if self.known_clients[client]["eol"] >= client_advert["eol"]:
                client_log.debug("Ignored already seen client advert")
                return
            self.known_clients[client]["timer"].cancel()
        except KeyError:
            client_log.info("New client")
            self.known_clients[client] = {}

        # Insert new client info and timeout into known clients
        def timeout_client():
            client_log.info("Client timed out")
            del self.known_clients[client]
            # TODO: redo fib
        timer = do_after(client_advert["eol"], timeout_client)
        self.known_clients[client]["timer"] = timer
        self.known_clients[client]["eol"] = client_advert["eol"]
        self.known_clients[client]["tags"] = client_advert["tags"]
        client_log.debug("New timeout: %s", to_human(client_advert["eol"]))

        # Insert new route score for this client via this peer
        if "routes" not in self.known_peers[peer]:
            self.known_peers[peer]["routes"] = {}
        self.known_peers[peer]["routes"][client] = client_advert["score"]
        client_log.debug("New route score: %s", client_advert["score"])

        # Insert new client advert into the client adverts priority queue
        deadline = time.time() + client_advert["ttp"]
        if client_advert["eol"] < deadline:
            client_log.debug("Not propagating advert as EOL < TTP")
            return
        client_item = {"client": client, "advert": client_advert}
        self.client_adverts_queue.put_nowait((deadline, client_item))
        client_log.debug("Advert deadline: %s", to_human(deadline))

    # Broadcast a peer advert containing as many pending client adverts as can
    # fit within ADVERT_CAPACITY bytes, taken in order of their TTP deadlines
    def broadcast_advert(self):
        log = ContextLogger(self.log, "Broadcast")

        self_advert_deadline = None
        eol = time.time() + self.ttl
        client_adverts = {}
        msg_len = len(encode_advert_msg(eol, client_adverts))
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

            # Handle this being a self peer advert by skipping a client advert
            # If this node is a client, insert a self client advert with an EOL
            if client_item == self.advert:
                self_advert_deadline = deadline
                if client_advert is None:
                    continue
                client_advert["eol"] = eol

            # Attempt to fit client advert into peer advert
            # Force it if this is the first (and as such only) client advert
            new_client_adverts = {**client_adverts, client: client_advert}
            new_msg_len = len(encode_advert_msg(eol, new_client_adverts))
            len_diff = new_msg_len - msg_len
            if len(client_adverts) != 0 and new_msg_len >= ADVERT_CAPACITY:
                log.debug("Not adding %s client advert (+%s bytes)", len_diff)
                self.client_adverts_queue.put_nowait((deadline, client_item))
                break
            log.debug("Added %s client advert (+%s bytes)", client, len_diff)
            client_adverts = new_client_adverts
            msg_len = new_msg_len

        # Send it!
        msg = encode_advert_msg(eol, client_adverts)
        self.udp.sendto(msg, ("<broadcast>", self.dport))
        log.debug("Sent %s bytes total with EOL %s", len(msg), to_human(eol))

        # If this broadcast counted as a self advert, queue the next one
        if self_advert_deadline is not None:
            log.debug("Did self advert due %s", to_human(self_advert_deadline))
            deadline = self_advert_deadline + self.ttl / self.tpf
            self.client_adverts_queue.put_nowait((deadline, self.advert))

        # Schedule next broadcast for self or remaining pending client adverts
        self.schedule_next_broadcast()

    # Determine when to schedule the next peer advert broadcast based on the
    # time until the deadline of our regular peer advert (TTL/TPF) or half of
    # of the time until the next client advert TTP deadline, whichever sooner
    def schedule_next_broadcast(self):

        # Find the time of the next advert deadline
        deadline, client_item = self.client_adverts_queue.get_nowait()
        self.client_adverts_queue.put_nowait((deadline, client_item))
        if client_item != self.advert:
            now = time.time()
            deadline = (deadline - now) / 2 + now

        # Replace previous broadcast task with new scheduled time
        if self.next_broadcast_task is not None:
            self.next_broadcast_task.cancel()
        self.next_broadcast_task = do_after(deadline, self.broadcast_advert)
        self.log.debug(
            "Next broadcast time set: %s (%s advert)",
            to_human(deadline), client_item["client"])


async def main():

    logging.basicConfig(
        format="%(asctime)s.%(msecs)04d [%(levelname)s] | %(message)s",
        level=logging.DEBUG, datefmt="%H:%M:%S:%m")

    import sys

    sport = int(sys.argv[1])
    dport = 33333
    ttl = 10
    tpf = 3

    client = None if len(sys.argv) == 2 else {
        "name": sys.argv[2],
        "ttp": float(sys.argv[3]),
        "tags": []
    }

    udp, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: UdpProtocol(sport, dport, ttl, tpf, client),
        local_addr=("0.0.0.0", sport), allow_broadcast=True)

    await asyncio.sleep(60)
    udp.close()


asyncio.run(main())

"""

async def start(
        port: int, server_port: int,
        ttl: float, tpf: int, ttp: float):

    # Start UDP and TCP servers
    udp_task = asyncio.create_task(start_udp())
    tcp_task = asyncio.create_task(start_tcp())
    task = asyncio.gather(udp_task, tcp_task)

    # Shutdown if we receive a signal
    loop = asyncio.get_running_loop()
    for sig in [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(sig, task.cancel)
    await task


async def subscribe(name, after):
    pass


async def publish():
    pass

"""
