# CrisisMesh

> **Decentralized, offline, peer-to-peer emergency communication network implemented in pure Java.**

CrisisMesh is a lightweight, zero-dependency mesh networking application designed to establish resilient local communication during emergencies and disasters when traditional telecom infrastructure and internet access fail.

---

## Key Features

- **P2P Flooding Protocol**: Messages propagate across neighboring nodes using epidemic-style mesh routing.
- **Loop & Duplicate Suppression**: Each broadcast has a globally unique UUID. Nodes track processed IDs and drop duplicates, preventing broadcast storms and infinite routing loops.
- **TTL (Time-To-Live)**: Hops are decremented per forward (`DEFAULT_TTL = 8`), bounding the lifetime and reach of messages in cyclical topologies.
- **Store-and-Forward**: If a peer is temporarily down or unreachable, unsent packets are queued in memory and retried automatically every 5 seconds.
- **Bidirectional Peer Discovery**: Seamless handshake via `HELLO|<listening_port>` packets allows nodes to automatically establish reciprocal links.
- **Dual User Interface**:
  - **Interactive CLI**: Fast, terminal-friendly interface.
  - **Desktop Swing GUI**: Clean graphical user interface with live message logs and peer inspector (enable with `--gui`).
- **Zero External Dependencies**: Implemented in standard Java SE with sockets, concurrent data structures, and Swing.

---

## Wire Protocol

CrisisMesh uses a clean delimited string wire format over TCP sockets:

```text
id|origin|type|text|timestamp|ttl|hops
```

- `id`: Globally unique UUID string.
- `origin`: Node name originating the message (Base64-encoded to prevent delimiter collisions).
- `type`: Message category (`SOS`, `RESOURCE_REQUEST`, `STATUS`, `INFO`).
- `text`: Message payload (Base64-encoded to support multi-line and special characters).
- `timestamp`: Epoch milliseconds when created.
- `ttl`: Remaining hops permitted.
- `hops`: Number of hops traveled so far.

---

## Getting Started

### Prerequisites

- Java Development Kit (JDK 8 or higher)

### Compilation

Compile the single-file source:

```bash
javac CrisisMesh.java
```

---

## Usage

```bash
java CrisisMesh <nodeName> <port> [host:port ...] [--gui]
```

- `<nodeName>`: Unique identifier for your node (e.g. `NodeA`, `Medic-1`, `HQ`).
- `<port>`: Local port number this node listens on (e.g. `5001`).
- `[host:port ...]`: Optional list of initial peers to connect to (e.g. `localhost:5002`).
- `[--gui]`: Optional flag to launch the Swing desktop interface instead of the CLI.

---

## Quick Demo: 3-Node Chain

Simulate a 3-node linear topology where Node A communicates with Node C through Node B (`A <-> B <-> C`).

Open 3 terminal windows:

### 1. Terminal 1 (Node B - Relay)
```bash
java CrisisMesh B 5002
```

### 2. Terminal 2 (Node A - Origin)
```bash
java CrisisMesh A 5001 localhost:5002
```

### 3. Terminal 3 (Node C - Destination)
```bash
java CrisisMesh C 5003 localhost:5002
```

### 4. Broadcast an SOS from Node A:
In Terminal A:
```text
A> sos Building 4 collapsed, 2 casualties reported
```

**Result:**
- Node A broadcasts the SOS.
- Node B receives it (`hops=1`) and forwards it to Node C.
- Node C receives it (`hops=2`):
  ```text
  >> [SOS] A (hops=2): Building 4 collapsed, 2 casualties reported
  ```

---

## CLI Commands

| Command | Description | Example |
| :--- | :--- | :--- |
| `sos <text>` | Broadcast a critical emergency alert | `sos Flood waters rising near Bridge 2` |
| `help <text>` | Submit a resource request | `help Medical kits and clean water needed` |
| `status <text>` | Report operational status | `status Shelter at capacity` |
| `info <text>` | Send informational update | `info Generator fuel replenished` |
| `connect <host:port>` | Manually connect to another peer | `connect 192.168.1.15:5001` |
| `peers` | Display connected neighbor sockets | `peers` |
| `inbox` | Print all received messages | `inbox` |
| `quit` | Gracefully shut down node | `quit` |

---

## Desktop GUI Mode

To launch with the Swing interface:

```bash
java CrisisMesh Alpha 5001 localhost:5002 --gui
```

---

## License

MIT License. Free for open-source and emergency response deployments.
