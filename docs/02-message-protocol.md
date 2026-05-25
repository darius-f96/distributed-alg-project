# Message Protocol

All inter-process and process-hub communication uses protobuf-serialized `Message` objects. The schema is in `distributed_alg.proto` at the repository root.

---

## Wire Format

```
[4 bytes big-endian int32: payload_length][payload_length bytes: serialized Message]
```

Sender: `DataOutputStream.writeInt(length)` then `write(payload)`.
Receiver: `DataInputStream.readInt()` then `readNBytes(size)`.

---

## The `Message` Wrapper

Every message — regardless of algorithm layer — is wrapped in the same `Message` envelope:

```proto
message Message {
    Type type = 1;           // Discriminator — tells you which payload field is set
    string messageUuid = 2;  // Tracing only, not used for routing
    string FromAbstractionId = 3;
    string ToAbstractionId = 4;
    string systemId = 5;

    NetworkMessage networkMessage = 6;
    // ... one payload field per Type value
    NnarRead nnarRead = 74;
    UcPropose ucPropose = 21;
    // etc.
}
```

There is no `oneof` — the `type` field acts as the discriminator. Only the field matching `type` is populated; all others are default/empty.

---

## `NetworkMessage` — Network Transit Wrapper

When a message travels over TCP between processes (or to/from hub), it is wrapped in a `NetworkMessage`:

```proto
message NetworkMessage {
    string senderHost = 1;
    int32 senderListeningPort = 2;
    Message message = 3;          // The actual payload
}
```

**Wrapping rule** (applied by `PerfectLink.wrapNetworkMessage`):

```
Outer Message {
    type: NETWORK_MESSAGE
    systemId: <current systemId>
    FromAbstractionId: <parentAbstractionId + ".pl">
    ToAbstractionId: <msg.ToAbstractionId>    ← preserved for destination routing
    NetworkMessage {
        senderHost: <this process's host>
        senderListeningPort: <this process's port>
        message: <inner msg — the PlSend payload>
    }
}
```

**Unwrapping rule** (applied by `PerfectLink.handleMessage` on `NETWORK_MESSAGE`):

```
Inner message extracted from NetworkMessage.message
Sender identified by (senderHost, senderListeningPort) lookup in processes list
Emits: PL_DELIVER { sender: <ProcessId>, message: <inner> }
Route to: parentAbstractionId
```

The `ToAbstractionId` on the outer wrapper is used by the TCP receiving side to dispatch to the right PL instance.

---

## Hub-Facing Messages

These messages flow between the hub and the process's `app` abstraction.

### Hub → Process

| Message type | Payload | Meaning |
|---|---|---|
| `PROC_INITIALIZE_SYSTEM` | `processes: [ProcessId...]` | System bootstrap; sent once per registration |
| `PROC_DESTROY_SYSTEM` | — | Tear down, deregister all abstractions |
| `APP_BROADCAST` | `value: Value` | Trigger BEB broadcast of this value |
| `APP_READ` | `register: string` | Read named register |
| `APP_WRITE` | `register: string, value: Value` | Write named register |
| `APP_PROPOSE` | `topic: string, value: Value` | Propose value on named consensus topic |

### Process → Hub

| Message type | Payload | Meaning |
|---|---|---|
| `PROC_REGISTRATION` | `owner, index` | Sent at startup to join the system |
| `APP_VALUE` | `value: Value` | BEB-delivered value forwarded to hub |
| `APP_READ_RETURN` | `register, value` | Read result |
| `APP_WRITE_RETURN` | `register` | Write acknowledgment |
| `APP_DECIDE` | `value: Value` | Consensus decision |

---

## Internal Messages (Never Leave a Process)

These messages flow between abstractions within the same process via the `messageQueue`.

### NNAR

| Type | Direction | Meaning |
|---|---|---|
| `NNAR_READ` | App → NNAR | Start a read operation |
| `NNAR_WRITE` | App → NNAR | Start a write operation |
| `NNAR_INTERNAL_READ` | NNAR → NNAR (via BEB) | Phase 1: ask all for their state |
| `NNAR_INTERNAL_VALUE` | NNAR → NNAR (via PL) | Phase 1 reply: here is my (ts, rank, value) |
| `NNAR_INTERNAL_WRITE` | NNAR → NNAR (via BEB) | Phase 2: write this value with this timestamp |
| `NNAR_INTERNAL_ACK` | NNAR → NNAR (via PL) | Phase 2 reply: written |
| `NNAR_READ_RETURN` | NNAR → App | Read complete, here is the value |
| `NNAR_WRITE_RETURN` | NNAR → App | Write complete |

### Consensus Stack

| Type | Direction | Meaning |
|---|---|---|
| `UC_PROPOSE` | App → UC | Propose a value for consensus |
| `UC_DECIDE` | UC → App | Consensus reached, here is the value |
| `EC_START_EPOCH` | EC → UC | Start new epoch with this (ts, leader) |
| `EP_PROPOSE` | UC → EP | Leader proposes value in this epoch |
| `EP_ABORT` | UC → EP | Abort current epoch |
| `EP_ABORTED` | EP → UC | Epoch aborted, here is last (ts, value) state |
| `EP_DECIDE` | EP → UC | Epoch decided on this value |
| `EP_INTERNAL_READ` | EP → EP (via BEB) | Phase 1: ask all for their state |
| `EP_INTERNAL_STATE` | EP → EP (via PL) | Phase 1 reply: here is my state |
| `EP_INTERNAL_WRITE` | EP → EP (via BEB) | Phase 2: write this value |
| `EP_INTERNAL_ACCEPT` | EP → EP (via PL) | Phase 2 reply: accepted |
| `EP_INTERNAL_DECIDED` | EP → EP (via BEB) | Decided |
| `EC_INTERNAL_NEW_EPOCH` | EC → EC (via BEB) | Leader broadcasts new epoch number |
| `EC_INTERNAL_NACK` | EC → EC (via PL) | Reject new epoch announcement |
| `ELD_TRUST` | ELD → EC | New leader elected |
| `EPFD_TIMEOUT` | EPFD → EPFD | Heartbeat timer fired |
| `EPFD_SUSPECT` | EPFD → ELD | Process unreachable |
| `EPFD_RESTORE` | EPFD → ELD | Process reachable again |
| `EPFD_INTERNAL_HEARTBEAT_REQUEST` | EPFD → EPFD (via PL) | Are you alive? |
| `EPFD_INTERNAL_HEARTBEAT_REPLY` | EPFD → EPFD (via PL) | Yes, alive |

### Link Primitives

| Type | Meaning |
|---|---|
| `PL_SEND` | Request to PerfectLink to send a message |
| `PL_DELIVER` | PerfectLink delivers received message to parent |
| `BEB_BROADCAST` | Request to BEB to broadcast |
| `BEB_DELIVER` | BEB delivers received message to parent |

---

## `ProcessId` Fields

```proto
message ProcessId {
    string host = 1;    // IP or hostname
    int32 port = 2;     // Listening port
    string owner = 3;   // Short alias (e.g., "abc")
    int32 index = 4;    // 1, 2, or 3
    int32 rank = 5;     // Assigned by hub; higher = preferred leader
}
```

Key/identity for a process is `owner + ":" + index` (used in ELD, EC) or `host + port` (used in PL for sender lookup).

---

## `Value` Fields

```proto
message Value {
    bool defined = 1;   // false = undefined/unset (default)
    int32 v = 2;        // The actual integer value
}
```

`defined = false` is the "bottom" value. Algorithms use `value.getDefined()` to check before using `v`.

---

## Abstraction ID Conventions

| Pattern | Meaning |
|---|---|
| `app` | Top-level application |
| `app.X` | Child abstraction X of app |
| `app.X.Y` | Child Y of X |
| `app.X[name]` | Named instance of X (e.g., per register or topic) |
| `app.uc[t].ep[3]` | Epoch 3 of consensus topic `t` |

The brackets `[...]` indicate a dynamic instance. The string inside is the disambiguator.
