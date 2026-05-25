# PerfectLink and BestEffortBroadcast

These two abstractions form the communication substrate. Every algorithm above them uses only `PL_SEND` / `PL_DELIVER` and `BEB_BROADCAST` / `BEB_DELIVER` — never raw sockets.

---

## PerfectLink (PL)

**File**: `src/main/java/mydist/process/abstraction/PerfectLink.java`

### What "Perfect" Means

In distributed systems theory, a Perfect Link guarantees:
- **Reliable delivery**: if sender is correct and sends to correct receiver, message is eventually delivered
- **No duplication**: each message delivered at most once
- **No creation**: only sent messages are delivered

In this implementation, reliability comes from TCP (handles retransmission) plus a 3-retry wrapper. Deduplication is not explicitly implemented — the system assumes the hub won't replay messages.

### Factory Pattern

PL is not registered as a singleton. Each parent abstraction gets its own PL **instance** via:

```java
PerfectLink pl = new PerfectLink(messageQ, processes, host, port, hubHost, hubPort, systemId);
abstractions.put("app.pl",        pl.createCopyWithParentAbstractionId("app"));
abstractions.put("app.beb.pl",    pl.createCopyWithParentAbstractionId("app.beb"));
abstractions.put("app.nnar[x].pl", pl.createCopyWithParentAbstractionId("app.nnar[x]"));
// etc.
```

`createCopyWithParentAbstractionId` creates a shallow copy sharing all network config, but sets `parentAbstractionId` differently. This determines where `PL_DELIVER` events are routed.

Source: `PerfectLink.java:38-42`.

### Handling `NETWORK_MESSAGE` (receive path)

```
Incoming NETWORK_MESSAGE
    │
    ├── extract inner message from NetworkMessage.message
    ├── find sender ProcessId by (senderHost, senderListeningPort) in processes list
    │   └── if not found: log warning, sender field left unset
    │
    └── emit PL_DELIVER {
            sender: <ProcessId>,
            message: <inner message>
        }
        ToAbstractionId: parentAbstractionId
```

Source: `PerfectLink.java:46-73`.

### Handling `PL_SEND` (send path)

```
PL_SEND received
    │
    ├── wrapNetworkMessage()
    │   └── Outer Message {
    │         type: NETWORK_MESSAGE
    │         systemId: <systemId>
    │         FromAbstractionId: parentAbstractionId + ".pl"
    │         ToAbstractionId: msg.ToAbstractionId
    │         NetworkMessage {
    │           senderHost: this.host
    │           senderListeningPort: this.port
    │           message: msg.PlSend.message
    │         }
    │       }
    │
    ├── determine destination:
    │   ├── PlSend.hasDestination() → use that ProcessId's host+port
    │   └── no destination → hub (hubHost, hubPort)
    │
    └── TCP send with 3-retry exponential backoff
        └── open socket, writeInt(length), write(payload), close
```

Source: `PerfectLink.java:85-137`.

**No destination = hub**: when a process replies to the hub (e.g., `APP_WRITE_RETURN`), the `PlSend.destination` is not set and the message goes to the hub address.

---

## BestEffortBroadcast (BEB)

**File**: `src/main/java/mydist/process/abstraction/BestEffortBroadcast.java`

### What BEB Guarantees

- **Best-effort validity**: if sender is correct, all correct processes eventually deliver
- **No duplication** (from underlying PL)
- **No creation** (from underlying PL)

Unlike reliable broadcast, BEB does not guarantee delivery if the sender crashes mid-broadcast.

### Handling `BEB_BROADCAST` (send path)

For each process in the system:
```
emit PL_SEND {
    ToAbstractionId: <this beb's abstraction id + ".pl">
    PlSend.destination: <process>
    PlSend.message: <bebBroadcast.message>
}
```

This produces N `PL_SEND` messages (one per process), each independently handled by the PL instance.

Source: `BestEffortBroadcast.java:29-44`.

### Handling `PL_DELIVER` (receive path)

```
emit BEB_DELIVER {
    sender: plDeliver.sender
    message: plDeliver.message
}
ToAbstractionId: plDeliver.message.ToAbstractionId
```

The destination of the `BEB_DELIVER` is taken from the **inner message's** `ToAbstractionId`, not the BEB's own ID. This means the algorithm that issued `BEB_BROADCAST` must set the inner message's `ToAbstractionId` to itself.

Source: `BestEffortBroadcast.java:46-58`.

---

## Interaction Pattern

Higher-level algorithms always interact with PL and BEB through the message queue, never by calling methods directly:

```
Algorithm wants to broadcast:
    offer(BEB_BROADCAST { ToAbstractionId: "app.nnar[x].beb", message: innerMsg })
        ↓ router dispatches to BestEffortBroadcast
        ↓ BEB offers N PL_SENDs to queue
        ↓ router dispatches each to PerfectLink
        ↓ PL sends over TCP

Remote process receives TCP:
    PL_DELIVER offered to queue
        ↓ router dispatches to "app.nnar[x].beb" (based on ToAbstractionId)
        ↓ BEB wraps in BEB_DELIVER, routes to innerMsg.ToAbstractionId
        ↓ algorithm handler called
```

---

## Known Quirk: Heartbeat Reply Destination

`EventuallyPerfectFailureDetector.sendHeartbeatReply` builds a `PL_SEND` without setting `PlSend.destination`. Since `destination` is missing, PL routes the reply to the **hub**, not back to the requesting process.

The heartbeat mechanism still works because the hub acts as a relay in this setup — but in a pure peer-to-peer setup without a hub, this would need to be fixed by capturing the sender from the `PL_DELIVER` and setting it as the destination.

Source: `EventuallyPerfectFailureDetector.java:75-93`.
