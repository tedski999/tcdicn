Requires cryptography module.

Assuming you have cloned the repository and are in the directory, run the example node implementation:

```bash
PYTHONPATH=. python3 ./examples/node.py
```

This node does not subscribe to or publish any data, but provides connectivity between other nodes in the network. As such, this implementation should be sufficient as the backbone of the network for most conceivable scenarios. If you want to run it on you PI during demonstrations, you can use Systemd to keep it running after you log off or even reboot:

```bash
# This file assumes this git repository is cloned to ~/tcdicn. Update it if otherwise
mkdir -p ~/.config/systemd/user/
cp ~/tcdicn/resources/tcdicn.service ~/.config/systemd/user/
loginctl enable-linger  # So our service stay running after logout
systemctl --user start tcdicn  # Start our service
systemctl --user status tcdicn  # View service status
```

To actually use the network, you need to implement your groups scenario's sensors and actuators as a tcdicn Node. Example sensor and actuator nodes have also been provided in `./examples``:

``` bash
TCDICN_ID=my_cool_sensor PYTHONPATH=. python3 ./examples/sensor.py
TCDICN_ID=my_cool_actuator PYTHONPATH=. python3 ./examples/actuator.py
```

Note that these "client" nodes should have unique names on the network, or you will significantly reduce the performance of the network.

After instantiating a Node with `node = Node()`, Your sensors and actuators only need to care about 3 methods:
- `await node.start(port: int, dport: int, ttl: float, tpf: float, client: dict = None)`: Starts the node networking servers and begins communicating with the network. Does not return until the server is shutdown (either press Ctrl+C, send a SIGINT to the process or cancel this coroutine in Python).
  - `port`: Which port to listen on for notifications, advertisements and TCP connections which send interests and data. Make sure this is accessable, as other nodes (potentially from other PIs) will try to contact this node using it.
  - `dport` (Discovery port): Which port is use by other nodes, which we send all of our notifications and advertisements to when broadcasting to discover each other. Most nodes should have this in common, but some nodes may set `port` differently if they want to run on the same device as another node (which is the "main" node on the device).
  - `ttl` (Time To Live) specifies how many seconds other nodes should remember you.
  - `tpf` (TTL PreFire) is how many notifications should be sent before our TTL runs out, such that notifications are sent to all peers every `ttl/tpf` seconds.
  - The optional `client` argument declares that this node is a "client" and should advertise this to other nodes. This argument is necessary for `node.get` and `node.set` to function. The `client` dict must contain the following keys:
    - `"name"`: The unique name given to this client on the network.
    - `"labels"`: A list of labels this client plans to publish to.
    - `"ttp"`: (Time To Propigate) Number of seconds to allow other nodes to delay before it must repeat our client advertisement to its peers (This allows nodes to "batch" together these advertisements that are broadcasted to their peers).
    - `"key"`: (Optional) The private key of this client in PEM format. Used for joining groups.
- `await node.get(label: str, ttl: float, tpf: float, ttp: float)`: Subscribe to some label for new data. Returns once data you have not seen before becomes available. Useful for actuators.
  - `ttl` (Time To Live) specifies how many seconds nodes should remember this interest for.
  - `tpf` (TTL PreFire) is how many interest notifications should be sent before the interest TTL runs out, such that notifications are sent to relavant peers every `ttl/tpf` seconds.
  - `ttp` (Time To Propigate): Number of seconds to allow other nodes to delay before it must repeat our interest to its relavant peers or fulfil our interest by sending us data (This allows nodes to "batch" together these messages into much fewer node-to-node TCP connections).
- `await node.set(label: str, data: str)`: Publish new labeled data to the network, which will only be propagated towards interested clients. Useful for sensors.

If you want to use encryption between clients in the same group, they only need to "join" with each other:
- `await node.join(group: str, client: str, key: bytes, labels: List[str]):` Publishes an invite to "{group}/{self.client}" for the other client to subscribe to. Reciprocally, this client subscribes to "{group}/{client}" to recieve their invite. These invites are validated with the provided public key of the other client. If both clients have a different key or if neither possess one yet, they keep the newer key.

If you would like to test locally with a virtual network of ICN nodes, run one of the example scenarios using Docker:

```bash
TCDICN_TTL=10 TCDICN_GET_TTL=30 TCDICN_VERBOSITY=info docker compose --file simulations/paths.yml up --build
```
Notice that you can configure many of the parameters passsed into the methods in the example implementations by setting them as enviroment variables.

Generate the keys needed to run the `groups.yml` simulation:
```bash
client=a-sensor sh -c 'mkdir -p keys && openssl genrsa -out keys/$client.pem 2048 && openssl rsa -in keys/$client.pem -pubout -out keys/$client'
client=a-actuator sh -c 'mkdir -p keys && openssl genrsa -out keys/$client.pem 2048 && openssl rsa -in keys/$client.pem -pubout -out keys/$client'
client=b-sensor sh -c 'mkdir -p keys && openssl genrsa -out keys/$client.pem 2048 && openssl rsa -in keys/$client.pem -pubout -out keys/$client'
client=b-actuator sh -c 'mkdir -p keys && openssl genrsa -out keys/$client.pem 2048 && openssl rsa -in keys/$client.pem -pubout -out keys/$client'
```
