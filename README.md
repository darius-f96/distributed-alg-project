# Distributed Algorithms — Reference Implementation

Java 21 implementation of two classic distributed computing algorithms: an **N-N Atomic Register** and **Uniform Consensus**. Built for a university course, graded 10/10.

Three student nodes connect to a teacher-provided hub. Together with three hub-side nodes, they form a six-node distributed system. The hub orchestrates test scenarios; the student nodes implement the algorithms.

---

## Algorithms

| Algorithm | Short name | What it does | Doc |
|-----------|-----------|--------------|-----|
| N-N Atomic Register | NNAR | Any process reads/writes; reads always see the most recent write (linearizability) | [docs/04-nnar.md](docs/04-nnar.md) |
| Uniform Consensus | UC | All correct processes decide the same value, even when leaders crash | [docs/06-consensus.md](docs/06-consensus.md) |

UC depends on three supporting algorithms:

| Algorithm | Short name | Role | Doc |
|-----------|-----------|------|-----|
| Eventually Perfect Failure Detector | EPFD | Heartbeat-based crash detection | [docs/05-failure-detection.md](docs/05-failure-detection.md) |
| Eventual Leader Detector | ELD | Elects highest-rank non-suspected process | [docs/05-failure-detection.md](docs/05-failure-detection.md) |
| Epoch Change | EC | Converts leader changes into epoch number increments | [docs/06-consensus.md](docs/06-consensus.md) |
| Epoch Consensus | EP | Single-epoch leader-based consensus | [docs/06-consensus.md](docs/06-consensus.md) |

All algorithms communicate via **Best-Effort Broadcast** and **Perfect Link** primitives built on TCP.

---

## Quickstart

```bash
# Build fat JAR
mvn package

# Run: 3 nodes connecting to hub at 127.0.0.1:5000, listening on ports 6001-6003
java -jar target/MyDist-1.0.jar 127.0.0.1 5000 127.0.0.1 6001 6002 6003

# Arguments: <hubHost> <hubPort> <nodeHost> <port1> [<port2> <port3>]
```

Reference hub binary is in `dalgs-reference-binaries/`. Owner alias is hardcoded as `"abc"` in `Main.java`; the hub assigns process ranks.

---

## Repository Tour

```
distributed_alg.proto                        All message types (single source of truth)
pom.xml                                      Maven: Java 21, protobuf-java 4.31.1, logback
src/main/java/mydist/
  Main.java                                  Entry: spawns 1-3 DistributedProcess
  process/
    DistributedProcess.java                  TCP listener + message router + abstraction registry
    abstraction/
      AbstractionLayer.java                  Interface: handleMessage(msg) + cleanup()
      App.java                               Hub bridge: translates hub commands ↔ internal events
      PerfectLink.java                       Reliable point-to-point link over TCP
      BestEffortBroadcast.java               Fan-out via PerfectLink to all processes
      NNAtomicRegister.java                  NNAR: quorum-based read/write
      consensus/
        EpState.java                         Record: (epoch timestamp, value)
        EventuallyPerfectFailureDetector.java Heartbeat timer + suspect/restore
        EventualLeaderDetector.java          Max-rank non-suspected process
        EpochChange.java                     Leader changes → epoch increments
        EpochConsensus.java                  Single epoch of consensus
        UniformConsensus.java               Multi-epoch uniform consensus
```

---

## Documentation

**Reading the code?** Start at [docs/01-architecture.md](docs/01-architecture.md) — explains the message bus, routing, and startup sequence. Then read algorithm docs in order.

**Using this as a reference to build a similar project?** Start at [docs/08-reimplementation-guide.md](docs/08-reimplementation-guide.md) — self-contained recipe with component checklist, wire format, hub contract, and common pitfalls.

| Doc | Contents |
|-----|----------|
| [01-architecture.md](docs/01-architecture.md) | Message bus, abstraction ID routing, wire protocol, startup sequence |
| [02-message-protocol.md](docs/02-message-protocol.md) | Protobuf schema, `Message` wrapper, `NetworkMessage` rules |
| [03-perfect-link.md](docs/03-perfect-link.md) | PerfectLink + BestEffortBroadcast implementation |
| [04-nnar.md](docs/04-nnar.md) | N-N Atomic Register: state, quorum flows, invariants |
| [05-failure-detection.md](docs/05-failure-detection.md) | EPFD heartbeat + ELD leader election |
| [06-consensus.md](docs/06-consensus.md) | EC + EP + UC: epoch stack, leader rotation, decide |
| [07-message-flow-traces.md](docs/07-message-flow-traces.md) | ASCII sequence diagrams for all major flows |
| [08-reimplementation-guide.md](docs/08-reimplementation-guide.md) | Recipe for reimplementing in any language |

---

## Tech Stack

- Java 21
- [protobuf-java 4.31.1](https://github.com/protocolbuffers/protobuf)
- [Logback 1.5.13](https://logback.qos.ch/) + SLF4J 2.0.9
- Maven with `maven-shade-plugin` (fat JAR)

---

## Reference

Algorithms based on:

> Cachin, C., Guerraoui, R., & Rodrigues, L. (2011). *Introduction to Reliable and Secure Distributed Programming* (2nd ed.). Springer.

Fail-Noisy system model (◇P): processes may crash, crashes are eventually detected accurately.
