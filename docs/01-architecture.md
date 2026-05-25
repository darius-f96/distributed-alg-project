# Architecture

## Overview

Each `DistributedProcess` is a self-contained node that:
1. Listens for incoming TCP connections
2. Deserializes protobuf `Message` objects
3. Routes messages to the correct `AbstractionLayer` handler
4. Lets handlers push outgoing messages back onto the same queue

**Everything flows through a single `BlockingQueue<Message>`.**

---

## Message Bus

```
Incoming TCP
    │
    ▼
handleClient()
  parse length-prefixed protobuf
    │
    ├── PROC_INITIALIZE_SYSTEM? ──► handleMessage() directly (bootstrap, not queued)
    │
    └── everything else ──────────► messageQueue.offer(msg)
                                            │
                                            ▼
                               processQueueThread.take()
                                            │
                                            ▼
                               handleMessage(msg)
                                 route by ToAbstractionId
                                            │
                                            ▼
                               abstractions.get(id).handleMessage(msg)
                                            │
                                            ▼
                               handler calls messageQueue.offer(outgoing)
                                            │
                                            └──► loops back to top
```

**Single-thread invariant**: all algorithm state mutations happen on `message-processing-thread-<index>`. No locks needed on algorithm fields. The only exception is `EventuallyPerfectFailureDetector`, which uses its own `ScheduledExecutorService` — it only calls `messageQueue.offer()`, never mutates shared state directly.

Sources: `DistributedProcess.java:90-135` (TCP listener), `DistributedProcess.java:119-135` (queue processor), `DistributedProcess.java:137-178` (router).

---

## Abstraction Registry

`DistributedProcess` maintains:
```java
Map<String, AbstractionLayer> abstractions
```

Keys are **abstraction ID strings**. Every message carries `ToAbstractionId` — the router calls `abstractions.get(toAbstraction).handleMessage(msg)`.

### Interface

```java
public interface AbstractionLayer {
    void handleMessage(DistributedAlg.Message msg);
    void cleanup();
}
```

All 11 concrete classes implement this.

---

## Abstraction ID Hierarchy

IDs form a tree. Parent-child relationships determine how `PL_DELIVER` events are routed upward.

```
app                                  App (hub bridge)
├── app.pl                           PerfectLink → hub
├── app.beb                          BestEffortBroadcast
│   └── app.beb.pl                   PerfectLink used by BEB
│
├── app.nnar[registerName]           NNAtomicRegister (one per register, lazy)
│   ├── app.nnar[x].pl
│   ├── app.nnar[x].beb
│   │   └── app.nnar[x].beb.pl
│
└── app.uc[topic]                    UniformConsensus (one per topic, lazy)
    ├── app.uc[t].ec                 EpochChange
    │   ├── app.uc[t].ec.pl
    │   ├── app.uc[t].ec.beb
    │   │   └── app.uc[t].ec.beb.pl
    │   └── app.uc[t].ec.eld        EventualLeaderDetector
    │       └── app.uc[t].ec.eld.epfd  EventuallyPerfectFailureDetector
    │           └── app.uc[t].ec.eld.epfd.pl
    │
    └── app.uc[t].ep[N]             EpochConsensus epoch N (dynamic, created per epoch)
        ├── app.uc[t].ep[N].beb
        │   └── app.uc[t].ep[N].beb.pl
        └── app.uc[t].ep[N].pl
```

`PerfectLink` is not a singleton — each parent abstraction gets its own instance (sharing network config, but routing `PL_DELIVER` to the right parent). Created via `pl.createCopyWithParentAbstractionId(parentId)`.

---

## Lazy Abstraction Registration

Base abstractions (`app`, `app.pl`, `app.beb`, `app.beb.pl`) are registered at `PROC_INITIALIZE_SYSTEM` time.

NNAR and UC abstractions are created **on first message arrival** (`DistributedProcess.java:161-169`):

```java
if (!abstractions.containsKey(toAbstraction) && toAbstraction.startsWith("app.nnar[")) {
    String key = extractRegisterFromAbstractionId(toAbstraction);
    registerNnarAbstractions(key);
} else if (!abstractions.containsKey(toAbstraction) && toAbstraction.startsWith("app.uc[")) {
    String topic = extractTopicFromAbstractionId(toAbstraction);
    registerConseusAbstractions(topic);
}
```

EP epoch abstractions are created dynamically inside `UniformConsensus.addEpAbstractions()` each time the epoch changes.

---

## Wire Protocol

All messages travel as length-prefixed protobuf over TCP:

```
Bytes 0-3:  big-endian int32 — payload length in bytes
Bytes 4+:   serialized Message proto
```

**Connection-per-message**: a new TCP socket is opened for each `PL_SEND`, flushed, and closed. No persistent connections.

**Retry policy**: 3 attempts, exponential backoff (100ms, 200ms). After 3 failures, exception is logged and the message is dropped. Source: `PerfectLink.java:103-134`.

---

## Startup Sequence

```
1. Main.java
   └── new DistributedProcess(owner="abc", index=1..3, host, port, hubHost, hubPort)
   └── executor.submit(proc::start)

2. proc.start()
   ├── registerToHub()
   │   └── send NETWORK_MESSAGE(PROC_REGISTRATION{owner, index}) to hub
   └── startTcpListener()
       └── ServerSocket.accept() loop on clientPool thread

3. Hub → PROC_INITIALIZE_SYSTEM{processes: [6 ProcessId entries with ranks]}
   (received as NETWORK_MESSAGE, handleClient detects it, calls handleMessage directly)

4. handleMessage(PROC_INITIALIZE_SYSTEM)
   ├── store systemId, processes list
   ├── find currentProcess in list by owner+index
   ├── registerAbstractions()  ← registers app, app.pl, app.beb, app.beb.pl
   └── startProcessingQueue()  ← starts message-processing-thread-<index>

5. Hub sends APP_READ / APP_WRITE / APP_PROPOSE / APP_BROADCAST
   └── these arrive as NETWORK_MESSAGE → queued → router
       └── router lazy-creates NNAR or UC abstractions on first access
```

---

## `ProcessId` and Ranks

Each process has:
- `host` + `port` — network address
- `owner` + `index` — identity (e.g., `"abc"`, `2`)
- `rank` — integer assigned by hub (used for leader election and tie-breaking)

The hub assigns ranks in `PROC_INITIALIZE_SYSTEM`. Higher rank = preferred leader.

---

## Message Structure

Every message is a protobuf `Message` with:
- `type` — enum discriminator
- `systemId` — identifies which consensus/register instance
- `FromAbstractionId` / `ToAbstractionId` — routing fields
- `messageUuid` — not used in routing, for tracing
- One optional payload field matching the type

For full schema details see [02-message-protocol.md](02-message-protocol.md).
