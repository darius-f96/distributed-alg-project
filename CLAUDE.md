# Distributed Algorithms Project

Java 21 implementation of N-N Atomic Register and Uniform Consensus. Three student nodes connect to a teacher hub. Graded 10/10.

## Documentation

All technical detail lives in `docs/`. Read in order for a tutorial path, or jump to the relevant file.

| File | Contents |
|---|---|
| [docs/01-architecture.md](docs/01-architecture.md) | Message bus, abstraction ID routing, wire protocol, startup |
| [docs/02-message-protocol.md](docs/02-message-protocol.md) | Proto schema, `Message` wrapper, hub message reference |
| [docs/03-perfect-link.md](docs/03-perfect-link.md) | PerfectLink + BestEffortBroadcast |
| [docs/04-nnar.md](docs/04-nnar.md) | N-N Atomic Register: quorum read/write algorithm |
| [docs/05-failure-detection.md](docs/05-failure-detection.md) | EPFD heartbeat + ELD leader election |
| [docs/06-consensus.md](docs/06-consensus.md) | EpochChange + EpochConsensus + UniformConsensus |
| [docs/07-message-flow-traces.md](docs/07-message-flow-traces.md) | ASCII sequence diagrams for all major flows |
| [docs/08-reimplementation-guide.md](docs/08-reimplementation-guide.md) | Recipe for building a compatible implementation |

**Building another project from this reference?** Start with `docs/08-reimplementation-guide.md`.

## Build and Run

```bash
mvn package
java -jar target/MyDist-1.0.jar <hubHost> <hubPort> <nodeHost> <port1> [<port2> <port3>]

# Example: 3 nodes connecting to hub at localhost:5000
java -jar target/MyDist-1.0.jar 127.0.0.1 5000 127.0.0.1 6001 6002 6003
```

Hub binary: `dalgs-reference-binaries/dalgs-linux-amd64` (or platform variant).

## Key Facts

- Single `BlockingQueue<Message>` per process — all algorithm state is single-threaded
- Abstraction ID strings (e.g., `app.uc[topic].ep[3].beb`) route messages to handlers
- NNAR and UC abstractions registered lazily on first message
- EPFD is the only component with a separate timer thread
- Wire protocol: 4-byte big-endian length prefix + protobuf bytes, TCP per-message
